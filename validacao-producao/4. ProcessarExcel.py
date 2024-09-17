import datetime
import os
import shutil
import sys
import psutil
import time
import pandas as pd
from typing import List
import BancoDados
import EnviarEmail
from joblib import Parallel, delayed
import script_html

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
            assunto = script_html.suporte_email.get('dados_falha').get('assunto')
            destinatario =script_html.suporte_email.get('dados_falha').get('destinatario')
            copias = script_html.suporte_email.get('dados_falha').get('copias')
            corpo = 'Processo foi interrompido na etapa inicial de procurar o arquivo.'
            corpo_html = script_html.html_falha

            EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html, arquivo_anexo='',caminho_anexo_xlsx='', nome_arquivo_xlsx='')

            sys.exit(f"Não foi possível encontrar o arquivo na pasta {self.caminho_pasta_origem}")
        
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
                assunto = script_html.suporte_email.get('dados_falha').get('assunto')
                destinatario =script_html.suporte_email.get('dados_falha').get('destinatario')
                copias = script_html.suporte_email.get('dados_falha').get('copias')
                corpo = 'Processo foi interrompido na segunda etapa de copiar o arquivo para pasta transitória.'
                corpo_html = script_html.html_falha

                EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html, arquivo_anexo='',caminho_anexo_xlsx='', nome_arquivo_xlsx='')

                sys.exit(f"Não foi possível encontrar o arquivo na pasta {self.caminho_pasta_origem} e copiar na pasta {self.caminho_destino}")

    def ler_e_concatenar_arquivos(self) -> pd.DataFrame:
        arquivos_excel = self.procurar_arquivos()
        self.copiar_arquivos(arquivos=arquivos_excel)
        
        if not arquivos_excel:
            assunto = script_html.suporte_email.get('dados_falha').get('assunto')
            destinatario =script_html.suporte_email.get('dados_falha').get('destinatario')
            copias = script_html.suporte_email.get('dados_falha').get('copias')
            corpo = 'Processo foi interrompido na etapa de gerar o dataframe.'
            corpo_html = script_html.html_falha

            EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html, arquivo_anexo='',caminho_anexo_xlsx='', nome_arquivo_xlsx='')

            sys.exit(f"Não foi possível gerar o dataframe pois os dados na pasta estão com erro.")
            
            # return pd.DataFrame()
            
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
        df.columns = df.columns.str.upper() #novo para teste

        colunas_necessarias = ['ID_APOLICE', 'CD_APOLICE', 'ID_ENDOSSO', 'NR_ENDOSSO', 'ID_PESSOA_CORRETOR', 'ID_PRODUTO','ID_CANAL', 'ID_HEAD_CANAL','ID_VERTICAL','ID_HEAD_VERTICAL','ID_SUB_VERTICAL','ID_HEAD_SUB_VERTICAL', 'ID_FILIAL', 'ID_LINHA_NEGOCIO','ID_PRODUTOR','PREMIO_TARIFARIO','PREMIO_ADICIONAL','TOTAL_PRODUCAO','MES_REFERENCIA']


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
            assunto = script_html.suporte_email.get('dados_falha').get('assunto')
            destinatario =script_html.suporte_email.get('dados_falha').get('destinatario')
            copias = script_html.suporte_email.get('dados_falha').get('copias')
            corpo = 'Processo foi interrompido na etapa de consultar os dados pois alguma coluna foi alterada.'
            corpo_html = script_html.html_falha

            EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html, arquivo_anexo='',caminho_anexo_xlsx='', nome_arquivo_xlsx='')

            sys.exit(f"Processo foi interrompido na etapa de consultar os dados pois alguma coluna foi alterada. Seguem as colunas que estão faltando: {colunas_faltantes}")


        df1.columns = df1.columns.str.upper()

        df2 = df1.astype(str)
        
        try:
            df2 = df2.rename(columns={'TOTAL PRODUÇÃO': 'TOTAL_PRODUCAO'})
        except KeyError:
            pass

        def remove_zeros(df2):
            # Colunas a serem ignoradas
            colunas_ignorar = ['PREMIO_TARIFARIO', 'PREMIO_ADICIONAL', 'TOTAL_PRODUCAO']
            
            # Verificar se df2 é um DataFrame válido
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("O argumento fornecido não é um DataFrame.")
            
            for col in df2.columns:
                if col not in colunas_ignorar:
                    # Verificar se a coluna é do tipo string
                    if pd.api.types.is_string_dtype(df2[col]):
                        # Verificar se a coluna contém '.0' como parte dos valores
                        if df2[col].str.contains('.0').any():
                            # Remover '.0' dos valores da coluna
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

        banco_gestao_comercial = BancoDados.BancoDeDados(dataframe=base_fria_limpo, nome_tabela='nome do database aqui')
        banco_producao = BancoDados.BancoDeDados(dataframe=base_fria_limpo, nome_tabela='nome do database aqu')

        query_linha_negocio = """
        SELECT
            idLinhaNegocio as ID_LINHA_NEGOCIO,
            LinhaNegocio as LINHA_NEGOCIO
        FROM [database].[dbo].[tabela]
        """
        linha_negocio = banco_producao.consultar(query_linha_negocio, 'nome do database aqu')
        linha_negocio['ID_LINHA_NEGOCIO'] = linha_negocio['ID_LINHA_NEGOCIO'].astype(str)

        query_produtores = """
        SELECT *
        FROM [EZZE_DdatabaseW_PRODUCAO].[dbo].[tabela]
        """
        dim_produtores = banco_producao.consultar(query_produtores, 'nome do database aqu')
        dim_produtores['id_pessoa'] = dim_produtores['id_pessoa'].astype(str)

        query_corretores = """
        SELECT *
        FROM [database].[dbo].[tabela]
        """
        dim_corretores = banco_producao.consultar(query_corretores, 'nome do database aqu')
        dim_corretores
        dim_corretores['id_Pessoa'] = dim_corretores['id_Pessoa'].astype(str)

        tabelas = [
            "CANAL","HEAD_CANAL","VERTICAL","HEAD_VERTICAL","SUB_VERTICAL","HEAD_SUB_VERTICAL","FILIAL"
        ]
        dataframes = {}

        def consultar_tabelas(nome_tabela):
            query = f"""
            SELECT *
            FROM [database].[dbo].[{nome_tabela}]
            """
            return banco_gestao_comercial.consultar(query, 'nome do database aqu')

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
                    assunto = script_html.suporte_email.get('dados_falha').get('assunto')
                    destinatario =script_html.suporte_email.get('dados_falha').get('destinatario')
                    copias = script_html.suporte_email.get('dados_falha').get('copias')
                    corpo = 'Falha no processo de validação das colunas Canal, Vertical, Produtor, Filial, Corretor. Rever processo e saber se alguma consulta com o banco de erro.'
                    corpo_html = script_html.html_falha

                    EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html, arquivo_anexo='',caminho_anexo_xlsx='', nome_arquivo_xlsx='')

                    sys.exit(f"Falha no processo de validação das colunas Canal, Vertical, Produtor, Filial, Corretor. Rever processo e saber se alguma consulta com o banco de erro.")

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
            base_fria_limpo['ID_PRODUTOR'] = base_fria_limpo['ID_PRODUTOR'].astype(str).apply(
                lambda x: x if pd.Series([x]).isin(dim_produtores['id_pessoa'].astype(str)).any() else valor_default
            )


        valor_default = '0'

        if 'ID_PESSOA_CORRETOR' in base_fria_limpo.columns:
            base_fria_limpo['ID_PESSOA_CORRETOR'] = base_fria_limpo['ID_PESSOA_CORRETOR'].apply(
                lambda x: x if x in dim_corretores['id_Pessoa'].values else valor_default
            )

        else:
            print("'ID_PESSOA_CORRETOR' não está presente na base_fria. A validação precisa ser feita no BI e no Excel.")

        return base_fria_limpo
    
    def enviar_dados(self):
        base_fria_limpo = self.validar_base()

        # Montar a lista de queries conforme você já fez
        base_fria_validada = pd.DataFrame(base_fria_limpo)
        base_fria_validada['DATA_CARGA'] = datetime.datetime.now().strftime("%Y-%m-%d")
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

        assunto = script_html.suporte_email.get('dados_sucesso').get('assunto')
        destinatario =script_html.suporte_email.get('dados_sucesso').get('destinatario')
        copias = script_html.suporte_email.get('dados_sucesso').get('copias')
        corpo = f'O processo deu certo e foi atualizado em {agora}'
        corpo_html = script_html.html_sucesso
        EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html,nome_arquivo_xlsx ='')
        
        #df_retorno = pd.DataFrame(lista_execucao_paralela)

        nova_lista = []
        for valores in lista_execucao_paralela:
            for i in valores:
                nova_lista.append(i)
        
        df_retorno = pd.DataFrame(nova_lista)
        df_retorno = df_retorno[df_retorno[0] =='ERRO']
        df_retorno = df_retorno.head(1000) # Aqui filtra somente mil mil linhas. 

        # Chamar a função para enviar e-mail com esses casos

        df_retorno.dropna()
        df_retorno.to_excel('arquivo_erros.xlsx', index=False)

    
        return df_retorno
    
    def tratar_erro_final(self):
        df_retorno = self.enviar_dados()
        # arquivo_erro = df_retorno[df_retorno=='ERRO']
        arquivo_erro = pd.read_excel('arquivo_erros.xlsx', index_col=False)

        if arquivo_erro.empty:
            print("Processo completo da Base Fria foi finalizado!")
            return

        agora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
        assunto = script_html.suporte_email.get('dados_falha').get('assunto')
        destinatario =script_html.suporte_email.get('dados_falha').get('destinatario')
        copias = script_html.suporte_email.get('dados_falha').get('copias')
        corpo = f'Foi realizado a atualização da base fria em {agora} mas gerou alguns erros. Em anexo o conteúdo que precisa ser tratado e na próxima atualização será ajustado.'
        corpo_html = script_html.html_falha
        caminho_anexo = 'arquivo_erros.xlsx'

        EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html, arquivo_anexo='',caminho_anexo_xlsx=caminho_anexo, nome_arquivo_xlsx='arquivo_erros.xlsx')

        # sys.exit(f"Foi realizado a atualização da base fria em {agora} mas gerou alguns erros. Em anexo o conteúdo que precisa ser tratado e na próxima atualização será ajustado.")

        return print("Processo completo da Base Fria foi finalizado!")
