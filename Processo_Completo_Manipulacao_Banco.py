
# Para manipulação do banco de dados
# Instalar: pip install pyodbc
import pyodbc

# Para manipulação do arquivo em Excel
import pandas as pd
import os

# Enviar informações em blocos
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

# Configurações de conexão com o banco de dados SQL Server
DB_CONFIG = {
    'driver': '{ODBC Driver 17 for SQL Server}', 
    'servidor': '',
    'banco': '',
    'usuario': '',
    'senha': ''
}

# Com base na minha função DB_CONFIG, vou coletar as credenciais, para não deixar exposta no script.
def conectar_banco():
    conexao_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['servidor']};"
        f"DATABASE={DB_CONFIG['banco']};"
        f"UID={DB_CONFIG['usuario']};"
        f"PWD={DB_CONFIG['senha']}"
    )
    conexao = pyodbc.connect(conexao_str)
    return conexao

def executar_consulta(query, params=None):
    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        resultados = cursor.fetchall()
        return resultados if resultados else [] 
    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")
        return [] 
    finally:
        cursor.close()
        conexao.close()

class ProcessadorExcel:

    def __init__(self, caminho_pasta):
        self.caminho_pasta = caminho_pasta
        self.dataframes = {}

    def encontrar_arquivo_excel(self):
        for nome_arquivo in os.listdir(self.caminho_pasta):
            if nome_arquivo.endswith('.xlsx') or nome_arquivo.endswith('.xls'):
                return os.path.join(self.caminho_pasta, nome_arquivo)
        return None

    def carregar_abas(self):
        arquivo_excel = self.encontrar_arquivo_excel()
        if arquivo_excel:
            xls = pd.ExcelFile(arquivo_excel)
            for nome_aba in xls.sheet_names:
                self.dataframes[nome_aba] = pd.read_excel(xls, sheet_name=nome_aba)
            return self.dataframes
        else:
            raise FileNotFoundError("Nenhum arquivo Excel encontrado na pasta especificada. Avaliar se não mudou algo")
        
class ManipuladorDataFrame:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def exibir(self):
        print(self.dataframe)

    def manipular(self):
        # Adicione aqui as funcionalidades de manipulação - Se precisar fazer mais alguma coisa. Por agora não precisa. Neste caso, vou criar a função e baixo e chamar aqui.
        pass

# Passar aqui caminho onde os arquivos ficarão armazenados.
caminho_pasta = fr'' 

# Chamar a função principal ai de cima para executar a separação de cada aba em arquivos dataframe
processador = ProcessadorExcel(caminho_pasta)

# Tratativa e separação dos arquivos. Objetivo aqui é saber se deu certo.
try:
    arquivos_excel = processador.carregar_abas()
    for nome_aba, df in arquivos_excel.items():
        #print(f"Manipulando a aba: {nome_aba}")
        manipulador = ManipuladorDataFrame(df)
        #manipulador.exibir() 
except FileNotFoundError as e:
    print(e)
# Fazer as validações de dados


# fazer uma consulta no banco e acessar a base
def consultar_dados_banco(query):
    # Já tenho o conector do banco criada
    conexao = conectar_banco()
    
    try:
        # Consulta ao banco de dados. Modificar a query aqui se precisar.
        consulta_query = f"""{query}
        
        """ 
        consultar_dados = pd.read_sql(consulta_query, conexao)

        # Verifica se a consulta retornou dados
        if not consultar_dados.empty:
            print("Dados consultados com sucesso:")
            #print(tabela_producao)
        else:
            print("Nenhum dado encontrado na consulta.")
    
    except Exception as e:
        print(f"Erro ao consultar os dados: {e}")
    
    finally:
        conexao.close()

    return consultar_dados

# Tratativa pensando em uma tabela especifica

query = """SELECT * FROM tabela"""

tabela = consultar_dados_banco(query)

tabela = pd.DataFrame(tabela)

# Deixar com o tipo string
tabela['coluna'] = tabela['coluna'].astype(str)

# Juntar as bases, mas só levar a coluna que não existir, neste caso preciso da coluna_x
grade_comercial = base_expandida.merge(tabela, on='coluna', how='left')

from typing import Literal

# Função que vai simplificar o processo
def validar_e_substituir_ids(grade_comercial : pd.DataFrame, base_recebida : pd.DataFrame, coluna_grade : str, coluna_base_recebida : str, id_default : str):
    grade_comercial[coluna_grade] = grade_comercial[coluna_grade].astype(str)
    base_recebida[coluna_base_recebida] = base_recebida[coluna_base_recebida].astype(str)

    grade_comercial[coluna_grade] = grade_comercial[coluna_grade].where(
        grade_comercial[coluna_grade].isin(base_recebida[coluna_base_recebida]), 
        id_default
    )
    
    return grade_comercial

# Caso em uso
query = """SELECT * FROM TABELA"""
base_query = consultar_dados_banco(query)
id_default = 3
coluna_grade = 'coluna'
coluna_base = 'coluna'
base_recebida.columns

tabela = validar_e_substituir_ids(grade_comercial, base_query, coluna_grade, coluna_base, id_default)
tabela

# Inserir os dados no servidor 3, no Database "NOME"

class InserirDados:
    def __init__(self, tabela, nome_tabela):
        self.tabela = tabela 
        self.nome_tabela = nome_tabela

    def inserir(self):
        if isinstance(self.tabela, pd.DataFrame) and not self.tabela.empty:
            # Verifica e cria a tabela se necessário. Sempre bom saber se em algum momento pagou e não foi criado uma nova
            self._criar_tabela_se_nao_existir()
            self._inserir_dataframe()

    def _criar_tabela_se_nao_existir(self):
        colunas = ', '.join([f"{col} NVARCHAR(255)" for col in self.tabela.columns]) 
        query = f"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.nome_tabela}') " \
                f"CREATE TABLE {self.nome_tabela} ({colunas})"
        
        conexao = conectar_banco()
        cursor = conexao.cursor()
        
        try:
            cursor.execute(query)
            conexao.commit()
            print(f"Tabela '{self.nome_tabela}' verificada e criada com sucesso.")
        except Exception as e:
            print(f"Erro ao criar a tabela: {e}")
        finally:
            cursor.close()
            conexao.close()

    def _inserir_dataframe(self):
        conexao = conectar_banco()
        cursor = conexao.cursor()
        
        try:
            # Apaga todos os registros antigos para não gerar linhas duplicadas
            deletar_query = f"TRUNCATE TABLE {self.nome_tabela}"
            cursor.execute(deletar_query)
            # Confirma a exclusão
            conexao.commit()  
            
            print(f"Todos os registros antigos da tabela '{self.nome_tabela}' foram apagados.")

            # Inserir os novos dados
            colunas = ', '.join(self.tabela.columns)
            valores = ', '.join(['?' for _ in range(len(self.tabela.columns))])
            insert_query = f"INSERT INTO {self.nome_tabela} ({colunas}) VALUES ({valores})"

            for index, linha in self.tabela.iterrows():
                cursor.execute(insert_query, tuple(linha))
            conexao.commit()
            print(f"Inseridos {len(self.tabela)} registros na tabela {self.nome_tabela}.")
        except Exception as e:
            print(f"Erro ao inserir os dados: {e}")
        finally:
            cursor.close()
            conexao.close()
