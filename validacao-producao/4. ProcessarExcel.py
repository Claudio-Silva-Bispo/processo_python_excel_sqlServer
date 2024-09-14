import datetime
import os
import shutil
import psutil
import time
import pandas as pd
from typing import List
import BancoDados
import EnviarEmail
from joblib import Parallel, delayed

class ProcessadorExcel:
    def __init__(self, caminho_pasta_origem: str, caminho_destino: str):
        self.caminho_pasta_origem = caminho_pasta_origem
        self.caminho_destino = caminho_destino

    def procurar_arquivos(self) -> List[str]:
        arquivos = [
            os.path.join(self.caminho_pasta_origem, arquivo)
            for arquivo in os.listdir(self.caminho_pasta_origem)
            if arquivo.endswith('.xlsx') or arquivo.endswith('.xls')
        ]
        
        if not arquivos:
            print("Nenhum arquivo Excel encontrado na pasta!")
            assunto = 'Processo Base Fria - Erro para encontrar o arquivo'
            corpo = fr'O primeiro processo falhou ao encontrar os arquivos na pasta de origem: {self.caminho_pasta_origem}'
            destinatario = ''
            copias = ''
            EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, arquivo_anexo='')
        
        return arquivos

    def copiar_arquivos(self, arquivos: List[str]) -> None:
        if not os.path.exists(self.caminho_destino):
            os.makedirs(self.caminho_destino)
        for arquivo in arquivos:
            try:
                shutil.copy(arquivo, self.caminho_destino)
                print(f"Arquivo {os.path.basename(arquivo)} copiado com sucesso para {self.caminho_destino}.")
            except Exception as e:
                print(f"Erro ao copiar {arquivo}: {e}. Enviar e-mail avisando que deu errado")
                assunto = 'Processo Base Fria - Erro para copiar os arquivos'
                corpo = fr'O Segundo processo falhou ao copiar os arquivos na pasta de destino: {self.caminho_destino}'
                destinatario = ''
                copias = ''
                EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, arquivo_anexo='')

    def ler_e_concatenar_arquivos(self) -> pd.DataFrame:
        arquivos_excel = self.procurar_arquivos()
        self.copiar_arquivos(arquivos=arquivos_excel)
        
        if not arquivos_excel:
            assunto = 'Processo Base Fria - Erro para gerar o dataframe'
            corpo = 'O Terceiro processo falhou ao encontrar os arquivos e compilar todos para gerar um único dataframe consolidado.'
            destinatario = ''
            copias = ''
            EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, arquivo_anexo='')
            return pd.DataFrame()  # Retorna um DataFrame vazio se nenhum arquivo for encontrado e enviar um e-mail avisando que não deu certo
            
        lista_dfs = [pd.read_excel(arquivo) for arquivo in arquivos_excel]
        consolidado_total = pd.concat(lista_dfs, ignore_index=True)
        
        return consolidado_total

    def fechar_processos_excel(self):
        for proc in psutil.process_iter(attrs=['pid', 'name']):
            if proc.info['name'] == "EXCEL.EXE":
                print(f"Encerrando processo Excel com PID: {proc.info['pid']}")  # Debug
                proc.terminate()  # Usar terminate() é mais seguro

    def limpar_pasta_destino(self):
        self.fechar_processos_excel()
        time.sleep(5)  # Aguarda um pouco para garantir que o processo foi encerrado

        for arquivo in os.listdir(self.caminho_destino):
            arquivo_completo = os.path.join(self.caminho_destino, arquivo)
            if os.path.isfile(arquivo_completo):
                try:
                    os.remove(arquivo_completo)
                    print(f"Arquivo {arquivo_completo} removido com sucesso.")
                except PermissionError:
                    print(f"Erro ao remover {arquivo_completo}.")

    def manipular_dataframe(self) -> pd.DataFrame:
        self.limpar_pasta_destino() 
        df = self.ler_e_concatenar_arquivos()

        colunas_necessarias = ['id_Apolice', 'cd_apolice', 'id_Endosso', 'nr_endosso', 'id_pessoa_corretor', 'id_produto','id_canal', 'Id_head_canal','id_vertical','id_Head_vertical','id_SUB_vertical','id_Head_Sub_vertical', 'id_filial', 'id_linha_negocio','id_Produtor','mes_referencia']


        # Primeira validação é verificar se todas as colunas estão presentes no modelo

        colunas_presentes = df.columns

        # Identificar colunas faltantes
        colunas_faltantes = [col for col in colunas_necessarias if col not in colunas_presentes]

        if colunas_faltantes:
            # Se houver colunas faltantes, gerar uma mensagem
            mensagem_faltantes = f"As seguintes colunas estão faltando: {', '.join(colunas_faltantes)}"
            print(mensagem_faltantes)
        else:
            mensagem_faltantes = None

        # Selecionar apenas as colunas necessárias, se todas estiverem presentes
        if not colunas_faltantes:
            df1 = df[colunas_necessarias]
        else:
            df1 = None
            print("Continuando o processo sem a seleção completa das colunas. Será enviado email para que ajuste os nomes")
            # chamar meu processo do e-mail aqui para enviar os dados
            assunto = 'Erro no processo de criação da Base Fria - Falta colunas padrões'
            destinatario = ''
            copias = ''
            corpo = "Falha no processo de criação pois alguma coluna foi modificada."
            EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo)


        df1.columns = df1.columns.str.upper()

        df2 = df1.astype(str)

        def remove_zeros(df2):
            for col in df2.columns:
                if df2[col].str.contains('.0').any():
                    df2[col] = df2[col].str.replace('.0', '', regex=False)
            return df2

        df3 = remove_zeros(df2)

        def remover_espacos(value):
            if isinstance(value, str):
                return value.strip()
            return value

        # Aplicar a função a cada célula do DataFrame
        base_fria_limpo = df3.map(remover_espacos)
  
        return base_fria_limpo

    def validar_base(self):
        base_fria_limpo = self.manipular_dataframe()
        self.fechar_processos_excel()
        self.limpar_pasta_destino()

        banco_gestao_comercial = BancoDados.BancoDeDados(dataframe=base_fria_limpo, nome_tabela='GESTAO_COMERCIAL')
        banco_producao = BancoDados.BancoDeDados(dataframe=base_fria_limpo, nome_tabela='DW_PRODUCAO')

        query_linha_negocio = """
        SELECT
            idLinhaNegocio as ID_LINHA_NEGOCIO,
            LinhaNegocio as LINHA_NEGOCIO
        FROM [database].[dbo].[tabela]
        """
        linha_negocio = banco_producao.consultar(query_linha_negocio, 'dw_producao')
        linha_negocio['ID_LINHA_NEGOCIO'] = linha_negocio['ID_LINHA_NEGOCIO'].astype(str)
        linha_negocio

        query_produtores = """
        SELECT *
        FROM [database].[dbo].[tabela]
        """
        dim_produtores = banco_producao.consultar(query_produtores, 'dw_producao')
        dim_produtores['id_pessoa'] = dim_produtores['id_pessoa'].astype(str)
        dim_produtores

        query_corretores = """
        SELECT *
        FROM [database].[dbo].[tabela]
        """
        dim_corretores = banco_producao.consultar(query_corretores, 'dw_producao')
        dim_corretores
        dim_corretores['id_pessoa'] = dim_corretores['id_pessoa'].astype(str)
        dim_corretores

        tabelas = [
            "CANAL","HEAD_CANAL","VERTICAL","HEAD_VERTICAL","SUB_VERTICAL","HEAD_SUB_VERTICAL","FILIAL"
        ]
        dataframes = {}

        def consultar_tabelas(nome_tabela):
            query = f"""
            SELECT *
            FROM [database].[dbo].[{nome_tabela}]
            """
            return banco_gestao_comercial.consultar(query, 'dw_producao')

        for nome_tabela in tabelas:
            df = consultar_tabelas(nome_tabela)
            df['tabela'] = nome_tabela
            dataframes[nome_tabela] = df

        def validar_ids(dataframes, base_fria_limpo):
            tabelas = [
                ("CANAL", "ID_CANAL", "ID_CANAL", 3),
                ("HEAD_CANAL", "ID_PESSOA_FENIX_HEAD_CANAL", "ID_HEAD_CANAL", 12345678),
                ("VERTICAL", "ID_VERTICAL", "ID_VERTICAL", 9),
                ("HEAD_VERTICAL", "ID_PESSOA_FENIX_HEAD_VERTICAL", "ID_HEAD_VERTICAL", 12345678),
                ("SUB_VERTICAL", "ID_SUB_VERTICAL", "ID_SUB_VERTICAL", 23),
                ("HEAD_SUB_VERTICAL", "ID_PESSOA_FENIX_HEAD_SUB", "ID_HEAD_SUB_VERTICAL", 12345678),
                ("FILIAL", "ID_FILIAL", "ID_FILIAL", 23)
            ]

            for tabela, coluna_grade, coluna_base_metas, id_default in tabelas:
                df = dataframes.get(tabela)
                if df is not None:
                    # Garantir que as colunas sejam do tipo string
                    df[coluna_grade] = df[coluna_grade].astype(str)
                    base_coluna = base_fria_limpo[coluna_base_metas].astype(str)

                    # Validar e substituir IDs
                    df[coluna_grade] = df[coluna_grade].where(
                        df[coluna_grade].isin(base_coluna),
                        id_default
                    )

                    #print(f"Dados validados para a tabela: {tabela}")
                    #print(df)
                else:
                    print(f"DataFrame para a tabela {tabela} não encontrado.")
                    assunto = 'Erro no processo de validação da Base Fria'
                    destinatario = ''
                    copias = ''
                    corpo = "Falha no processo de validação das colunas Canal, Vertical, Produtor, Filial, Corretor. Rever processo e saber se alguma consulta com o banco de erro."
                    EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo)

        validar_ids(dataframes, base_fria_limpo=base_fria_limpo)

        base_fria_limpo.copy

        meses_pt = {
            '1': 'janeiro',
            '2': 'fevereiro',
            '3': 'março',
            '4': 'abril',
            '5': 'maio',
            '6': 'junho',
            '7': 'julho',
            '8': 'agosto',
            '9': 'setembro',
            '10': 'outubro',
            '11': 'novembro',
            '12': 'dezembro',
            '13':'nao_definido'
        }

        base_fria_limpo['NOME_MES'] = base_fria_limpo['MES_REFERENCIA'].map(meses_pt)
        base_fria_limpo

        # Validações finais

        valor_default = '0'

        if 'ID_LINHA_NEGOCIO' in base_fria_limpo.columns:
            base_fria_limpo['ID_LINHA_NEGOCIO'] = base_fria_limpo['ID_LINHA_NEGOCIO'].apply(
                lambda x: x if x in linha_negocio['ID_LINHA_NEGOCIO'].values else valor_default
            )

        else:
            print("'ID_LINHA_NEGOCIO' não está presente na base_fria. A validação precisa ser feita no BI e no Excel.")
            # Não irei chamar e-mail aqui pois esses dados podem ser visto no BI da gestão comercial, apenas filtrando os IDs presentes.


        valor_default = '12345678'

        if 'ID_PRODUTOR' in base_fria_limpo.columns:
            base_fria_limpo['ID_PRODUTOR'] = base_fria_limpo['ID_PRODUTOR'].apply(
                lambda x: x if x in dim_produtores['id_pessoa'].values else valor_default
            )

        else:
            print("'ID_PRODUTOR' não está presente na base_fria. A validação precisa ser feita no BI e no Excel.")


        valor_default = '0'

        if 'ID_PESSOA_CORRETOR' in base_fria_limpo.columns:
            base_fria_limpo['ID_PESSOA_CORRETOR'] = base_fria_limpo['ID_PESSOA_CORRETOR'].apply(
                lambda x: x if x in dim_corretores['id_pessoa'].values else valor_default
            )

        else:
            print("'ID_PRODUTOR' não está presente na base_fria. A validação precisa ser feita no BI e no Excel.")

        return base_fria_limpo
    
    def enviar_dados(self):
        base_fria_limpo = self.validar_base()

        # Montar a lista de queries conforme você já fez

        base_fria_validada = pd.DataFrame(base_fria_limpo)
        lista_valores_final = base_fria_validada.values.tolist()

        querys_full = []
        for linha in lista_valores_final:
            dados_query = ', '.join([f"'{x}'" for x in linha])
            query = f"INSERT INTO BASE_FRIA VALUES ({dados_query})"
            querys_full.append(query)

        # Dividir em pacotes
        pacotes = []
        subpacote = []
        for query in querys_full:
            subpacote.append(query)
            if len(subpacote) % 10000 == 0:
                pacotes.append(subpacote)
                #print(len(subpacote))
                subpacote = []

        pacotes.append(subpacote)
        pacotes_importacao = pacotes

        # Executar a inserção em paralelo
        banco = BancoDados.BancoDeDados(dataframe=base_fria_validada, nome_tabela='BASE_FRIA')
        banco._criar()
        banco._truncar()

        print("Iniciar processo de inserir os dados...")
        lista_execucao_paralela = Parallel(n_jobs=5)(delayed(banco._inserir_dados)(i) for i in pacotes_importacao)
        print("Dados inseridos com sucesso!")

        agora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        assunto = 'Sucesso na atualização da Base Fria - Gestão Comercial'
        destinatario = ''
        copias = ''
        corpo = fr"Foi realizado a atualização da base fria em {agora}"
        EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, arquivo_anexo='')
        df_retorno = pd.DataFrame(lista_execucao_paralela)
    
        return df_retorno
    
    def tratar_erro_final(self):
        df_retorno = self.enviar_dados()
        arquivo_erro = df_retorno[df_retorno=='ERRO']

        import tempfile

        def salvar_dataframe_em_arquivo_temporario(df: pd.DataFrame) -> str:
            """ Salva o DataFrame em um arquivo temporário e retorna o caminho do arquivo. """
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_file:
                df.to_excel(temp_file.name, index=False)
                return temp_file.name
            
        arquivo_temporario = salvar_dataframe_em_arquivo_temporario(arquivo_erro)
        agora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        # Enviar os erros
        assunto = 'Tratar os erros gerados na atualização da Base Fria'
        destinatario = ''
        copias = ''
        corpo = fr"Foi realizado a atualização da base fria em {agora} mas gerou alguns erros. Em anexo o conteúdo que precisa ser tratado e na próxima atualização será ajustado."

        EnviarEmail.enviar_email(
            assunto=assunto, 
            destinatario=destinatario, 
            copias=copias, 
            corpo=corpo, 
            arquivo_anexo=arquivo_temporario)

        return print("Processo completo da Base Fria foi finalizado!")
