#--------------------( Importação das biliotecas )--------------------#

# Para manipulação do banco de dados
# Instalar: pip install pyodbc
import pyodbc

# Para manipulação do arquivo em Excel
import pandas as pd
import os
import shutil

# Enviar informações em blocos
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

# Personalização das funções e documentar com tipos esperados e formatações necessárias que ajudem outros usuários.
from typing import Literal, List, Any

# Tirar os warnings desnecessários
import warnings

# Suprimir todos os warnings
warnings.filterwarnings("ignore")

from itertools import product

from joblib import Parallel, delayed

import autenticacao_banco

# Para encerrar o processo do excel uma vez que eu tiver o dataframe
import psutil

#--------------------( Criação das funções para o projeto )--------------------#

class BancoDeDados:
    def __init__(self):
        # Carregar configurações de conexão do módulo de autenticação
        self.db_config = autenticacao_banco.DB_CONFIG
    
    def conectar(self, nome_tabela: Literal['dw_producao', 'gestao_comercial']) -> pyodbc.Connection:
        config = self.db_config[nome_tabela]
        conexao_str = (
            f"DRIVER={config['driver']};"
            f"SERVER={config['servidor']};"
            f"DATABASE={config['banco']};"
            f"UID={config['usuario']};"
            f"PWD={config['senha']}"
        )
        return pyodbc.connect(conexao_str)

    def consultar(self, query: str, nome_tabela: Literal['']) -> pd.DataFrame:
        conexao = self.conectar(nome_tabela)
        try:
            consultar_dados = pd.read_sql(query, conexao)
            if not consultar_dados.empty:
                print("Dados consultados com sucesso:")
            else:
                print("Nenhum dado encontrado na consulta.")
        except Exception as e:
            print(f"Erro ao consultar os dados: {e}")
            return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
        finally:
            conexao.close()
        
        return consultar_dados

    def inserir(self, pacote: List[str]) -> List[List[Any]]:
        logs = []
        try:
            conexao = self.conectar('')
            cursor = conexao.cursor()
            for i, insert in enumerate(pacote):
                try:
                    cursor.execute(insert)
                    logs.append(['SUCESSO', "OK", "OK"])
                except Exception as e:
                    logs.append(['ERRO', e, insert])
                
                if (i + 1) % 10000 == 0:
                    cursor.commit()
            
            cursor.commit()
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")
        finally:
            cursor.close()
            conexao.close()
        
        return logs


class ProcessadorExcel:

    def __init__(self, caminho_pasta : Literal[''], caminho_destino: str):
        self.caminho_pasta = caminho_pasta
        self.caminho_destino = caminho_destino
        self.dataframes = {}

    def encontrar_arquivo_excel(self):
        for nome_arquivo in os.listdir(self.caminho_pasta):
            if nome_arquivo.endswith('.xlsx') or nome_arquivo.endswith('.xls'):
                return os.path.join(self.caminho_pasta, nome_arquivo)
        return None
    
    def copiar_arquivo(self):
        arquivo_excel = self.encontrar_arquivo_excel()
        if arquivo_excel:
            # Copiar o arquivo para a pasta de destino
            shutil.copy(arquivo_excel, self.caminho_destino)
            return os.path.join(self.caminho_destino, os.path.basename(arquivo_excel))
        else:
            raise FileNotFoundError("Nenhum arquivo Excel encontrado na pasta!")

        
    def carregar_abas(self):
        arquivo_copiado = self.copiar_arquivo()  # Usar o arquivo copiado e não o original
        xls = pd.ExcelFile(arquivo_copiado)
        for nome_aba in xls.sheet_names:
            self.dataframes[nome_aba] = pd.read_excel(xls, sheet_name=nome_aba)
        return self.dataframes
    
    def fechar_processos_excel(self):
        for proc in psutil.process_iter():
            if proc.name() == "EXCEL.EXE":
                proc.kill()
        
    def limpar_pasta_destino(self):
        self.fechar_processos_excel()

        for arquivo in os.listdir(self.caminho_destino):
            arquivo_completo = os.path.join(self.caminho_destino, arquivo)
            if os.path.isfile(arquivo_completo):
                # os.remove(arquivo_completo)
                try:
                    os.remove(arquivo_completo)
                    print(f"Arquivo {arquivo_completo} removido com sucesso.")
                except PermissionError:
                    print(f"Erro ao remover {arquivo_completo}.")
        

class ManipuladorDataFrame:
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def exibir(self):
        print(self.dataframe)

    def manipular_linha_negocio(self, banco: BancoDeDados):
        # Consultar dados de linha de negócio
        query_linha_negocio = """
        SELECT
            idLinhaNegocio as ID_LINHA_NEGOCIO,
            LinhaNegocio as LINHA_NEGOCIO
        FROM tabela
        """
        self.linha_negocio = banco.consultar(query_linha_negocio, '')
        self.linha_negocio['ID_LINHA_NEGOCIO'] = self.linha_negocio['ID_LINHA_NEGOCIO'].astype(str)

    def manipular_produtores(self, banco: BancoDeDados):
        # Consultar dados da tabela Produtor
        query_produtores = "SELECT * FROM tabela"
        self.dim_produtores = banco.consultar(query_produtores, '')

    def processar(self):
        # Ajustar as colunas e renomear
        self.dataframe = self.dataframe[['']]
        
        # Renomear as colunas
        self.dataframe = self.dataframe.rename(columns={
            ''
        })

        # Conversões e manipulações
        self.dataframe['LINHA_NEGOCIO'] = self.dataframe['LINHA_NEGOCIO'].str.upper()
        self.dataframe['FLAG_PRODUTOR_ESPECIALISTA'] = self.dataframe['ID_PRODUTOR_ESPECIALISTA'].notnull().astype(bool)
        self.dataframe.replace('-', None, inplace=True)

        # Processar DataFrame conforme sua lógica
        df_novo = self.dataframe.copy()
        df_cor = df_novo[df_novo.FLAG_PRODUTOR_ESPECIALISTA == False]
        df_esp = df_novo[df_novo.FLAG_PRODUTOR_ESPECIALISTA == True][['ID_PESSOA_CORRETOR', 'ID_PRODUTOR_ESPECIALISTA', 'LINHA_NEGOCIO']]
        
        # Manipulações adicionais
        df_esp['LINHA_NEGOCIO'] = df_esp['LINHA_NEGOCIO'].str.split(';')
        df_esp = df_esp.explode('LINHA_NEGOCIO').reset_index(drop=True)
        df_esp['LINHA_NEGOCIO'] = df_esp['LINHA_NEGOCIO'].str.strip()

        df_esp = df_esp.merge(self.linha_negocio, how='left', left_on='LINHA_NEGOCIO', right_on='LINHA_NEGOCIO')
        df_esp = df_esp[df_esp.LINHA_NEGOCIO.notnull()]

        # Filtragem e criação do DataFrame final
        lista_produtores = list(self.dim_produtores['id_pessoa'])
        df_esp = df_esp[df_esp['ID_PRODUTOR_ESPECIALISTA'].isin(lista_produtores)]

        # Criação do DataFrame cartesiano
        ids_linha = list(self.linha_negocio['ID_LINHA_NEGOCIO'])
        ids_corretores = list(df_novo['ID_PESSOA_CORRETOR'])
        cartesiano_corretores = list(product(ids_corretores, ids_linha))

        df_final = pd.DataFrame(cartesiano_corretores, columns=['ID_PESSOA_CORRETOR', 'ID_LINHA_NEGOCIO'])
        df_grade = df_final.merge(df_novo, how='left', on='ID_PESSOA_CORRETOR')

        # Função para verificar produtores
        def verificar_produtor(id: int):
            default = 12345678
            return id if id in lista_produtores else default

        df_grade['ID_PRODUTOR_PRINCIPAL_VALIDADO'] = df_grade['ID_PRODUTOR_PRINCIPAL'].apply(verificar_produtor)

        # Preparar o DataFrame final
        df_grade = df_grade[['']]

        df_final_grade = df_grade.merge(df_esp, how='left', on=['ID_PESSOA_CORRETOR', 'ID_LINHA_NEGOCIO'])
        df_final_grade[['ID_PRODUTOR_ESPECIALISTA', 'LINHA_NEGOCIO']] = df_final_grade[['ID_PRODUTOR_ESPECIALISTA', 'LINHA_NEGOCIO']].fillna('-')
        df_final_grade['ID_PRODUTOR'] = df_final_grade.apply(lambda linha: linha['ID_PRODUTOR_ESPECIALISTA'] if linha['LINHA_NEGOCIO'] != '-' else linha['ID_PRODUTOR_PRINCIPAL_VALIDADO'], axis=1)                                        

        # Armazenar o resultado final
        self.grade_comercial = df_final_grade

        # Verifique se a coluna 'ID_LINHA_NEGOCIO' está presente
        print("Colunas de df_esp:", df_final_grade.columns)


    def validar_e_substituir_ids(self, base_recebida: pd.DataFrame, coluna_grade: str, coluna_base_recebida: str, id_default: str):
        self.dataframe[coluna_grade] = self.dataframe[coluna_grade].astype(str)
        base_recebida[coluna_base_recebida] = base_recebida[coluna_base_recebida].astype(str)

        self.dataframe[coluna_grade] = self.dataframe[coluna_grade].where(
            self.dataframe[coluna_grade].isin(base_recebida[coluna_base_recebida]),
            id_default
        )
    
    def validar_ids(self, banco: BancoDeDados, df_final_grade: pd.DataFrame):

        # Atualizar o DataFrame a ser validado
        self.dataframe = df_final_grade.copy()

        # Validação para cada tabela conforme seu código
        tabelas = [
            ("CANAL", "ID_CANAL", "ID_CANAL", 3),
            ("HEAD_CANAL", "ID_HEAD_CANAL", "ID_PESSOA_FENIX_HEAD_CANAL", 12345678),
            ("VERTICAL", "ID_VERTICAL", "ID_VERTICAL", 9),
            ("HEAD_VERTICAL", "ID_HEAD_VERTICAL", "ID_PESSOA_FENIX_HEAD_VERTICAL", 12345678),
            ("SUB_VERTICAL", "ID_SUB_VERTICAL", "ID_SUB_VERTICAL", 23),
            ("HEAD_SUB_VERTICAL", "ID_HEAD_SUB_VERTICAL", "ID_PESSOA_FENIX_HEAD_SUB", 12345678),
            ("FILIAL", "ID_FILIAL", "ID_FILIAL", 23)
        ]

        for tabela, coluna_grade, coluna_base, id_default in tabelas:
            query = f"SELECT * FROM {tabela}"
            base_recebida = banco.consultar(query, '')
            self.validar_e_substituir_ids(base_recebida, coluna_grade, coluna_base, id_default)


        # Ajustar ID_ASSESSORIA e ID_LINHA_NEGOCIO
        if 'ID_ASSESSORIA' in self.dataframe.columns:
            self.dataframe['ID_ASSESSORIA'] = self.dataframe['ID_ASSESSORIA'].replace("-", "0").fillna("0")

        if 'ID_LINHA_NEGOCIO' in self.dataframe.columns:
            self.dataframe['ID_LINHA_NEGOCIO'] = self.dataframe['ID_LINHA_NEGOCIO'].replace("-", "0").fillna("0")

        if 'ID_PRODUTOR_ESPECIALISTA' in self.dataframe.columns:
            self.dataframe['ID_PRODUTOR_ESPECIALISTA'] = self.dataframe['ID_PRODUTOR_ESPECIALISTA'].replace("-", "12345678").fillna("12345678")

        if 'LINHA_NEGOCIO' in self.dataframe.columns:
            self.dataframe['LINHA_NEGOCIO'] = self.dataframe['LINHA_NEGOCIO'].replace("-", "0").fillna("0")

        # preciso confirmar que deu certo
        return self.dataframe 


class GerenciadorErros:
    def enviar_email_erro(erro):
        print("Enviando e-mail de erro...")
        # Vu pegar nosso script que já foi criado para fazer isso.

#--------------------( Manipular o excel )--------------------#

# Passar aqui caminho onde os arquivos ficarão armazenados.
caminho_pasta = fr''

caminho_destino = '' 

# Chamar a função principal ai de cima para executar a separação de cada aba em arquivos dataframe
processador = ProcessadorExcel(caminho_pasta=caminho_pasta, caminho_destino=caminho_destino)

# Tratativa e separação dos arquivos. Objetivo aqui é saber se deu certo.
try:
    arquivos_excel = processador.carregar_abas()
    for nome_aba, df in arquivos_excel.items():
        #print(f"Manipulando a aba: {nome_aba}")
        manipulador = ManipuladorDataFrame(df)
        #manipulador.exibir() 
except FileNotFoundError as e:
    print(e)


# Fluxo de Execução principal
# if __name__ == "__main__":
banco = BancoDeDados()

dataframe = pd.DataFrame(df)
manipulador = ManipuladorDataFrame(dataframe)

# No seu fluxo principal
# Passo 1: Manipular dados
manipulador.manipular_linha_negocio(banco)
manipulador.manipular_produtores(banco)

# Passo 2: Processar dados
manipulador.processar()

# Passo 3: Validar IDs
df_final_grade = manipulador.grade_comercial
df_final_grade_validado = manipulador.validar_ids(banco, df_final_grade)



lista_valores_final = df_final_grade_validado.values.tolist()

querys_full = []
for linha in lista_valores_final:
    #print(linha)
    ID_PESSOA_CORRETOR = linha[0]
    ID_LINHA_NEGOCIO = linha[1]
    AGRUPAMENTO = linha[2]
    ID_PESSOA_CORRETOR_AGRUPAMENTO = linha[3]
    ID_CANAL = linha[4]
    ID_HEAD_CANAL = linha[5]
    ID_VERTICAL = linha[6]
    ID_HEAD_VERTICAL = linha[7]
    ID_SUB_VERTICAL = linha[8]
    ID_HEAD_SUB_VERTICAL = linha[9]
    ID_FILIAL = linha[10]
    ID_PRODUTOR_PRINCIPAL = linha[11]
    ID_ASSESSORIA = linha[12]
    ID_GRUPO = linha[13]
    ID_PRODUTOR_PRINCIPAL_VALIDADO = linha[14]
    ID_PRODUTOR_ESPECIALISTA = linha[15]
    LINHA_NEGOCIO = linha[16]
    ID_PRODUTOR = linha[17]
    
    dados_query = [f"'{x}'" for x in linha]
    dados_query = ', '.join(dados_query)
    query =f"insert into TABELA values ({dados_query})"
    querys_full.append(query)


#--------------------( Inserir dados no banco )--------------------#

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

banco = BancoDeDados()
con = banco.conectar(nome_tabela='gestao_comercial')
lista_execucao_paralela =Parallel(n_jobs= 5)(delayed(banco.inserir)(i) for i in pacotes_importacao)
df_retorno = pd.DataFrame(lista_execucao_paralela[0])
df_retorno[df_retorno[0]=='ERRO']


# Limpar a pasta no final para não deixar arquivos.
caminho_destino = '' 

limpar = ProcessadorExcel(caminho_pasta='', caminho_destino=caminho_destino)
limpar.limpar_pasta_destino()
