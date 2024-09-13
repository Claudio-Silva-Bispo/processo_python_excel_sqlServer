
# Para manipulação do banco de dados
# Instalar: pip install pyodbc
import pyodbc

# Personalização das funções e documentar com tipos esperados e formatações necessárias que ajudem outros usuários.
from typing import Literal, List, Any

# Para manipulação do arquivo em Excel
import pandas as pd

# Chamar demais componentes
import Autenticacoes

class BancoDeDados:
    def __init__(self,dataframe : pd.DataFrame, nome_tabela : Literal['inserir aqui o exemplo do database'] ):

        # Carregar configurações de conexão do módulo de autenticação
        self.db_config = Autenticacoes.DB_CONFIG
        self.dataframe = dataframe
        self.nome_tabela = nome_tabela
    
    def conectar(self, nome_database: Literal['database', 'database']) -> pyodbc.Connection:
        config = self.db_config[nome_database]
        conexao_str = (
            f"DRIVER={config['driver']};"
            f"SERVER={config['servidor']};"
            f"DATABASE={config['banco']};"
            f"UID={config['usuario']};"
            f"PWD={config['senha']}"
        )
        return pyodbc.connect(conexao_str)

    def consultar(self, query: str, nome_database: Literal['database', 'database']) -> pd.DataFrame:
        conexao = self.conectar(nome_database)
        try:
            consultar_dados = pd.read_sql(query, conexao)
            if not consultar_dados.empty:
                print("Dados consultados com sucesso:")
            else:
                print("Nenhum dado encontrado na consulta.")
        except Exception as e:
            print(f"Erro ao consultar os dados: {e}")
            return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro para tratarmos
        finally:
            conexao.close()
        
        return consultar_dados
    
    def inserir(self, pacotes: List[List[str]]) -> List[List[Any]]:
        if isinstance(self.dataframe, pd.DataFrame) and not self.dataframe.empty:
            self._criar()
            self._truncar()
            return self._inserir_dados(pacotes)
            

    def _criar(self):
        colunas = ', '.join([f"{col} NVARCHAR(255)" for col in self.dataframe.columns]) 
        query = f"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.nome_tabela}') " \
                f"CREATE TABLE {self.nome_tabela} ({colunas})"
        
        conexao = self.conectar('gestao_comercial')
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

    def _truncar(self):
        try:
            conexao = self.conectar('database')
            cursor = conexao.cursor()
            delete_query = f"TRUNCATE TABLE {self.nome_tabela}"
            cursor.execute(delete_query)
            conexao.commit()
            print(f"Todos os registros antigos da tabela '{self.nome_tabela}' foram apagados.")
        except Exception as e:
            print(f"Erro ao truncar a tabela: {e}")
        finally:
            cursor.close()
            conexao.close()

    def _inserir_dados(self, pacote: List[str]) -> List[List[Any]]:
        logs = []
        try:
            conexao = self.conectar('database')
            cursor = conexao.cursor()
            for i, insert in enumerate(pacote):
                try:
                    cursor.execute(insert)
                    logs.append(['SUCESSO', "OK", "OK"])
                    print("Dados inseridos {i}")
                except Exception as e:
                    logs.append(['ERRO', e, insert])
                    print(f"Erro ao inserir: {insert} - Erro: {e}")
                
                if (i + 1) % 10000 == 0:
                    conexao.commit()  # Use a conexão para commit

            conexao.commit()  # Commit final
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")
        finally:
            cursor.close()
            conexao.close()
        
        return logs

# Montar a lista de queries conforme você já fez
from joblib import Parallel, delayed


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

# Tratar os erros e inserir em um arquivo do excel para enviar no e-mail
df_retorno = pd.DataFrame(lista_execucao_paralela)
df_retorno[df_retorno=='ERRO']
