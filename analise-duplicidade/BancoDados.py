
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
    def __init__(self,dataframe : pd.DataFrame, nome_tabela : Literal['nome_exemplo'] ):

        # Carregar configurações de conexão do módulo de autenticação
        self.db_config = Autenticacoes.DB_CONFIG
        self.dataframe = dataframe
        self.nome_tabela = nome_tabela
    
    def conectar(self, nome_database: Literal['nome_database', 'nome_database']) -> pyodbc.Connection:
        config = self.db_config[nome_database]
        conexao_str = (
            f"DRIVER={config['driver']};"
            f"SERVER={config['servidor']};"
            f"DATABASE={config['banco']};"
            f"UID={config['usuario']};"
            f"PWD={config['senha']}"
        )
        return pyodbc.connect(conexao_str)

    def consultar(self, query: str, nome_database: Literal['nome_database', 'nome_database']) -> pd.DataFrame:
        conexao = self.conectar(nome_database)
        try:
            consultar_dados = pd.read_sql(query, conexao)
            if not consultar_dados.empty:
                print(fr"Dados consultados com sucesso: {query}")
            else:
                print("Nenhum dado encontrado na consulta.")
        except Exception as e:
            print(f"Erro ao consultar os dados: {e}")
            return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro para tratarmos
        finally:
            conexao.close()
        
        return consultar_dados
    
    def chamar_procedure(self, nome_procedure: str) -> pd.DataFrame:
        conexao = self.conectar('nome_database')
        cursor = conexao.cursor()
        try:
            cursor.execute(f"EXEC {nome_procedure}")
            rows = cursor.fetchall()
            
            # Depuração: Imprimir algumas linhas dos dados retornados
            print(f"Primeiras linhas retornadas: {rows[:5]}")
            
            # Extrair apenas os nomes das colunas
            colunas = [desc for desc in cursor.description]
            print(f"Colunas retornadas: {colunas}")
            print(f"Número de colunas retornadas: {len(colunas)}")
            
            # Criar DataFrame com as colunas corretas
            df = pd.DataFrame.from_records(rows, columns=colunas)
            print(f"Procedure {nome_procedure} executada com sucesso.")
        except Exception as e:
            print(f"Erro ao executar a procedure: {e}")
            return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro para tratarmos
        finally:
            cursor.close()
            conexao.close()
        
        return df
    
    # df = banco.chamar_procedure('nome_procedure')
    
    def inserir(self, pacotes: List[List[str]]) -> List[List[Any]]:
        if isinstance(self.dataframe, pd.DataFrame) and not self.dataframe.empty:
            self._criar()
            self._truncar()
            return self._inserir_dados(pacotes)
            

    def _criar(self):
        colunas = ', '.join([f"{col} NVARCHAR(255)" for col in self.dataframe.columns]) 
        query = f"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.nome_tabela}') " \
                f"CREATE TABLE {self.nome_tabela} ({colunas})"
        
        conexao = self.conectar('nome_database')
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
            conexao = self.conectar('gestao_comercial')
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
            conexao = self.conectar('gestao_comercial')
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