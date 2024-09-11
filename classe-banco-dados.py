class BancoDeDados:
    def __init__(self):
        # Carregar configurações de conexão do módulo de autenticação
        self.db_config = autenticacao_banco.DB_CONFIG
    
    def conectar(self, nome_tabela: Literal['nome', 'nome-dois']) -> pyodbc.Connection:
        config = self.db_config[nome_tabela]
        conexao_str = (
            f"DRIVER={config['driver']};"
            f"SERVER={config['servidor']};"
            f"DATABASE={config['banco']};"
            f"UID={config['usuario']};"
            f"PWD={config['senha']}"
        )
        return pyodbc.connect(conexao_str)

    def consultar(self, query: str, nome_tabela: Literal['nome', 'nome-doisl']) -> pd.DataFrame:
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
            conexao = self.conectar('nome')
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
