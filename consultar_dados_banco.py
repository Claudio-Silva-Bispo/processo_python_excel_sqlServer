def consultar(self, query: str, nome_tabela: Literal['nome', 'nome_dois']) -> pd.DataFrame:
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
