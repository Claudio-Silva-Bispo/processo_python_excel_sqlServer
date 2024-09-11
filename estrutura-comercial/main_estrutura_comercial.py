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
import time

# Chamar a classe que vai procurar os arquivos na pasta
import ProcessadorExcel 

# Para manipular o arquivo do excel
import ManipuladorDataFrame

# Acessar o banco de dados
import ConectarBanco

# Inserir dados no banco
import InserirDadosBanco

#--------------------( Classe para manipular os arquivos no excel )--------------------#
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
         # Usar o arquivo copiado e não o original
        arquivo_copiado = self.copiar_arquivo() 
        xls = pd.ExcelFile(arquivo_copiado)
        for nome_aba in xls.sheet_names:
            self.dataframes[nome_aba] = pd.read_excel(xls, sheet_name=nome_aba)

        xls.close()
        return self.dataframes
    
    def fechar_processos_excel(self):
        for proc in psutil.process_iter():
            if proc.name() == "EXCEL.EXE":
                proc.kill()
        
    def limpar_pasta_destino(self):
        self.fechar_processos_excel()
        time.sleep(5)

        for arquivo in os.listdir(self.caminho_destino):
            arquivo_completo = os.path.join(self.caminho_destino, arquivo)
            if os.path.isfile(arquivo_completo):
                # os.remove(arquivo_completo)
                try:
                    os.remove(arquivo_completo)
                    print(f"Arquivo {arquivo_completo} removido com sucesso.")
                except PermissionError:
                    print(f"Erro ao remover {arquivo_completo}.")
        
# Passar aqui caminho onde os arquivos ficarão armazenados.
caminho_pasta = fr''
caminho_destino = fr''

# Chamar a função principal ai de cima para executar a separação de cada aba em arquivos dataframe
processador = ProcessadorExcel(caminho_pasta, caminho_destino)

# Tratativa e separação dos arquivos. Objetivo aqui é saber se deu certo.
try:
    dataframes = processador.carregar_abas()
    for nome_aba, df in dataframes.items():
        #print(f"Manipulando a aba: {nome_aba}")
        manipulador = ManipuladorDataFrame.ManipuladorDataFrame(df)
        #manipulador.exibir() 
except FileNotFoundError as e:
    print(e)


class BancoDados:

    def __init__(self, dataframe, nome_tabela: str, nome_conexao: str):
        self.db_config = autenticacao_banco.DB_CONFIG
        self.dataframe = dataframe 
        self.nome_tabela = nome_tabela
        self.nome_conexao = nome_conexao  # Novo atributo para armazenar o nome da conexão

    def conectar(self) -> pyodbc.Connection:
        try:
            config = self.db_config[self.nome_conexao]
        except KeyError:
            print(f"Erro: a conexão '{self.nome_conexao}' não está configurada.")
            print("Chaves disponíveis:", self.db_config.keys())
            raise  # Re-raise the exception to stop execution if needed

        conexao_str = (
            f"DRIVER={config['driver']};"
            f"SERVER={config['servidor']};"
            f"DATABASE={config['banco']};"
            f"UID={config['usuario']};"
            f"PWD={config['senha']}"
        )
        return pyodbc.connect(conexao_str)

    def inserir(self):
        if isinstance(self.dataframe, pd.DataFrame) and not self.dataframe.empty:
            self._criar()
            self._inserir_dataframe()

    def _criar(self):
        colunas = ', '.join([f"{col} NVARCHAR(255)" for col in self.dataframe.columns]) 
        query = f"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.nome_tabela}') " \
                f"CREATE TABLE {self.nome_tabela} ({colunas})"
        
        conexao = self.conectar()
        cursor = conexao.cursor()
        
        try:
            cursor.execute(query)
            conexao.commit()
            print(f"Tabela '{self.nome_tabela}' verificada/criada com sucesso.")
        except Exception as e:
            print(f"Erro ao criar a tabela: {e}")
        finally:
            cursor.close()
            conexao.close()

    def _inserir_dataframe(self):
        conexao = self.conectar()
        cursor = conexao.cursor()
        
        try:
            delete_query = f"TRUNCATE TABLE {self.nome_tabela}"
            cursor.execute(delete_query)
            conexao.commit()
            
            print(f"Todos os registros antigos da tabela '{self.nome_tabela}' foram apagados.")

            colunas = ', '.join(self.dataframe.columns)
            valores = ', '.join(['?' for _ in range(len(self.dataframe.columns))])
            insert_query = f"INSERT INTO {self.nome_tabela} ({colunas}) VALUES ({valores})"

            for index, linha in self.dataframe.iterrows():
                cursor.execute(insert_query, tuple(linha))
            conexao.commit()
            print(f"Inseridos {len(self.dataframe)} registros na tabela {self.nome_tabela}.")
        except Exception as e:
            print(f"Erro ao inserir os dados: {e}")
        finally:
            cursor.close()
            conexao.close()

# Inserir os dados conforme a lista de tabelas e seus dataframes.
tabelas_e_dataframes = [
    ('RESUMO', dataframes.get('RESUMO')),
    ('CANAL', dataframes.get('CANAL')),
    ('HEAD_CANAL', dataframes.get('HEAD_CANAL')),
    ('VERTICAL', dataframes.get('VERTICAL')),
    ('HEAD_VERTICAL', dataframes.get('HEAD_VERTICAL')),
    ('SUB_VERTICAL', dataframes.get('SUB_VERTICAL')),
    ('HEAD_SUB_VERTICAL', dataframes.get('HEAD_SUB_VERTICAL')),
    ('FILIAL', dataframes.get('FILIAL')),
    ('PRODUTOR', dataframes.get('PRODUTOR')),
    ('DEPARA_LINHA_NEGOCIO', dataframes.get('DEPARA_LINHA_NEGOCIO'))
]

# Insira os dados conforme a lista de tabelas e seus dataframes.
for nome_tabela, dataframe in tabelas_e_dataframes:
    if dataframe is not None: 
        nome_conexao = "gestao_comercial"
        inserir_dados = BancoDados(dataframe, nome_tabela, nome_conexao)
        inserir_dados.inserir()
        #print(f"Dados inseridos na tabela: {nome_tabela}")
    #else:
        #print(f"DataFrame para a tabela {nome_tabela} não encontrado.")

# Enviar somente a base consolidada
base_consolidada = dataframes.get('BASE_CONSOLIDADA')
base_consolidada['DATA_BASE'] = pd.to_datetime(base_consolidada['DATA_BASE'], errors='coerce')
inserir_dados = BancoDados(dataframe, nome_tabela, nome_conexao)

limpar = ProcessadorExcel(caminho_pasta='', caminho_destino=caminho_destino)
limpar.limpar_pasta_destino()
