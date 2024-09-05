# fazer uma consulta no banco e acessar a base de "Produção atual"

def consultar_dados():
    # Já tenho o conector do banco criada
    conexao = conectar_banco()
    
    try:
        # Consulta ao banco de dados. Modificar a query aqui se precisar.
        consulta_query = f"""SELECT
        colunas1,
        coluna2
        FROM tabela
        """ 
        tabela_producao = pd.read_sql(consulta_query, conexao)

        # Verifica se a consulta retornou dados
        if not df.empty:
            print("Dados consultados com sucesso:")
            #print(tabela_producao)
        else:
            print("Nenhum dado encontrado na consulta.")
    
    except Exception as e:
        print(f"Erro ao consultar os dados: {e}")
    
    finally:
        conexao.close()

    return tabela_producao


tabela_producao = consultar_dados()
