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
            print(f"Tabela '{self.nome_tabela}' verificada/ criada com sucesso.")
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
