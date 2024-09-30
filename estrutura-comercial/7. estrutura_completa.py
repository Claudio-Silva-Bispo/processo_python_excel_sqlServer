#--------------------( Importação das biliotecas )--------------------#

# Para manipulação do banco de dados
# Instalar: pip install pyodbc
import pyodbc

# Para manipulação do arquivo em Excel
import pandas as pd
import os
import shutil

# Personalização das funções e documentar com tipos esperados e formatações necessárias que ajudem outros usuários.
from typing import Literal, List, Any

# Tirar os warnings desnecessários
import warnings

# Suprimir todos os warnings
warnings.filterwarnings("ignore")

from itertools import product

from joblib import Parallel, delayed

import autenticacoes

# Para encerrar o processo do excel uma vez que eu tiver o dataframe
import psutil
import time

#--------------------( Classe para manipular os arquivos no excel )--------------------#
class ProcessadorExcel:

    def __init__(self, caminho_pasta : Literal['inserir caminho do arquivo aqui'], caminho_destino: str):
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

class ManipuladorDataFrame:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def exibir(self):
        print(self.dataframe)

    def manipular(self):
        # Adicione aqui as funcionalidades de manipulação - Se precisar fazer mais alguma coisa. Por agora não precisa.
        pass
        
# Passar aqui caminho onde os arquivos ficarão armazenados.
caminho_pasta = fr'inserir rota aqui'
caminho_destino = fr'inserir rota aqui'

# Chamar a função principal ai de cima para executar a separação de cada aba em arquivos dataframe
processador = ProcessadorExcel(caminho_pasta, caminho_destino)

# Tratativa e separação dos arquivos. Objetivo aqui é saber se deu certo.
try:
    dataframes = processador.carregar_abas()
    for nome_aba, df in dataframes.items():
        #print(f"Manipulando a aba: {nome_aba}")
        manipulador = ManipuladorDataFrame(df)
except FileNotFoundError as e:
    print(e)


class BancoDados:

    def __init__(self, dataframe, nome_tabela: str, nome_conexao: str):
        self.db_config = autenticacoes.DB_CONFIG
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

def funcao_principal_estrutura():

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
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        if dataframe is not None: 
            dataframe['DATA_ATUALIZACAO'] = agora
            nome_conexao = "nome_aqui"
            inserir_dados = BancoDadosEstruturaComercial.BancoDados(dataframe, nome_tabela, nome_conexao)
            inserir_dados.inserir()
            print(f"Dados inseridos na tabela: {nome_tabela}")
        else:
            print(f"DataFrame para a tabela {nome_tabela} não encontrado.")
            assunto = f'Verificar processo de atualização da ESTRUTURA COMERCIAL. Não foi possível inserir as informações no banco/n. DataFrame para a tabela {nome_tabela} não encontrado.'
            destinatario =script_html.suporte_email.get('dados_falha_estrutura').get('destinatario')
            copias = script_html.suporte_email.get('dados_falha_estrutura').get('copias')
            corpo = 'Processo foi interrompido na etapa inicial de procurar o arquivo.'
            corpo_html = script_html.html_falha

            EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html, arquivo_anexo='',caminho_anexo_xlsx='', nome_arquivo_xlsx='')

    # Enviar somente a base consolidada
    base_consolidada = dataframes.get('BASE_CONSOLIDADA')
    base_consolidada
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    base_consolidada['DATA_ATUALIZACAO'] = agora

    if any(base_consolidada.columns.str.startswith('Unnamed')):
        base_consolidada = base_consolidada.loc[:, ~base_consolidada.columns.str.startswith('Unnamed')]

    try:
        inserir_dados = BancoDadosEstruturaComercial.BancoDados(dataframe=base_consolidada, nome_tabela='METAS', nome_conexao='nome_aqui')
        inserir_dados.inserir()
    except Exception as e:
        print(f"Erro ao inserir dados na tabela METAS: {e}")

    limpar = ProcessadorExcel.ProcessadorExcel(caminho_pasta='', caminho_destino=caminho_destino)
    limpar.limpar_pasta_destino()
    print("Processo para criar a estrutura da Gestão Comercial finalizou.")

    assunto = script_html.suporte_email.get('dados_sucesso_estrutura').get('assunto')
    destinatario =script_html.suporte_email.get('dados_sucesso_estrutura').get('destinatario')
    copias = script_html.suporte_email.get('dados_sucesso_estrutura').get('copias')
    corpo = 'Processo para criar a ESTRUTURA da Gestão Comercial finalizou.'
    corpo_html = script_html.html_sucesso_estrutura

    EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html,caminho_anexo_xlsx=None, nome_arquivo_xlsx='')

    print("E-mail enviado para todos sinalizando sobre a finalização do processo.")

funcao_principal_estrutura()
print("Processo para criar a estrutura da Gestão Comercial finalizou.")
