def conectar(self, nome_tabela: Literal['nome', 'nome_dois']) -> pyodbc.Connection:
        config = self.db_config[nome_tabela]
        conexao_str = (
            f"DRIVER={config['driver']};"
            f"SERVER={config['servidor']};"
            f"DATABASE={config['banco']};"
            f"UID={config['usuario']};"
            f"PWD={config['senha']}"
        )
        return pyodbc.connect(conexao_str)
