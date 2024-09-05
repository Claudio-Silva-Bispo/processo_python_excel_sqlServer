# Passar aqui caminho onde os arquivos ficarão armazenados.
caminho_pasta = fr''

# Chamar a função principal ai de cima para executar a separação de cada aba em arquivos dataframe
processador = ProcessadorExcel(caminho_pasta)

# Tratativa e separação dos arquivos. Objetivo aqui é saber se deu certo.
try:
    arquivos_excel = processador.carregar_abas()
    for nome_aba, df in arquivos_excel.items():
        #print(f"Manipulando a aba: {nome_aba}")
        manipulador = ManipuladorDataFrame(df)
        #manipulador.exibir() 
except FileNotFoundError as e:
    print(e)

# Ajustar a tabela e deixar só as tabelas que irei usar
df = df[['Chave_I4pro Corretor', 'Agrupamento_Tipo', 'Agrup_Chave Corr Principal', 'N_CANAL', 'CODFENIX_HEAD_CANAL', 'N_Vertical', 'CODFENIX_HEAD_Vertical','N_Sub_Vertical','CODFENIX_HEAD_Sub_Vertical','N_Filial','CODFENIX_Comercial_Principal','CODFENIX_Comercial_Especialista','Produto_comercial_especialista','CODFENIX_Assessoria','GRUPO']]

# Ajustar os nomes das colunas
df = df.rename(columns={'Chave_I4pro Corretor': 'ID_PESSOA_CORRETOR','Agrupamento_Tipo':'AGRUPAMENTO','Agrup_Chave Corr Principal':'ID_PESSOA_CORRETOR_AGRUPAMENTO','N_CANAL':'ID_CANAL','CODFENIX_HEAD_CANAL':'ID_HEAD_CANAL','N_Vertical':'ID_VERTICAL','CODFENIX_HEAD_Vertical':'ID_HEAD_VERTICAL','N_Sub_Vertical':'ID_SUB_VERTICAL','CODFENIX_HEAD_Sub_Vertical':'ID_HEAD_SUB_VERTICAL','N_Filial':'ID_FILIAL','CODFENIX_Comercial_Principal':'ID_PRODUTOR_PRINCIPAL','CODFENIX_Comercial_Especialista':'ID_PRODUTOR_ESPECIALISTA','Produto_comercial_especialista':'LINHA_NEGOCIO','CODFENIX_Assessoria':'ID_ASSESSORIA','GRUPO':'ID_GRUPO'})

# Converter a coluna 'PRODUTO_PRODUTOR_ESPECIALISTA' para maiuscula
df['LINHA_NEGOCIO'] = df['LINHA_NEGOCIO'].str.upper()

## # Criar uma nova coluna na tabela df chamada 'FLAG_PRODUTOR_ESPECIALISTA' e verificar se existe informação na coluna 'ID_PRODUTOR_ESPECIALISTA'
df['FLAG_PRODUTOR_ESPECIALISTA'] = None

# Avaliar todaa a tabela e se tiver "-" em qualquer linha, trocar por null ou None
df = df.replace('-', None)

# Verifica se na tabela df existe informação na coluna 'ID_PRODUTOR_ESPECIALISTA', se sim, inserir TRUE (1), se não FALSE(0)
df['FLAG_PRODUTOR_ESPECIALISTA'] = df['ID_PRODUTOR_ESPECIALISTA'].notnull().astype(bool)

# Se a coluna "LINHA_NEGOCIO" tiver mais de uma informação que for separada por ";", preciso criar outras colunas começando com "PRODUTO_PRODUTOR_ESPECIALISTA_1", "PRODUTO_PRODUTOR_ESPECIALISTA_2" e assim por diante
if 'LINHA_NEGOCIO' in df.columns:
    # Divide os valores da coluna em várias colunas
    produtos_divididos = df['LINHA_NEGOCIO'].str.split(';', expand=True)

    # Renomeia as colunas geradas para o modelo que informei
    produtos_divididos.columns = [f'LINHA_NEGOCIO_{i + 1}' for i in range(produtos_divididos.shape[1])]

    # Concatena as novas colunas a tabela original
    df = pd.concat([df, produtos_divididos], axis=1)


# Filtrar colunas que começam com 'PRODUTO_PRODUTOR_ESPECIALISTA' só para eu ter certeza que deu certo minha divisão
for col in df.columns:
    if col.startswith('PRODUTO_PRODUTOR_ESPECIALISTA'):
        print(col)

