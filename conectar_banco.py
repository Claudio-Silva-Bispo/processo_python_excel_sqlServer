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
