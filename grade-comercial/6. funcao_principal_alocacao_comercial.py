
# Alocação da grade comercial
import ManipuladorDataFrameAlocacaoComercial
import BancoDeDadosAlocacaoComercial
import ProcessadorExcel

# Tirar os warnings desnecessários
import warnings

# Suprimir todos os warnings
warnings.filterwarnings("ignore")

# Demais bibliotecas
import pandas as pd
from joblib import Parallel, delayed
from datetime import datetime

def funcao_principal_alocacao_comercial():
    # Passar aqui caminho onde os arquivos ficarão armazenados.
    caminho_pasta = fr'informar a rota aqui'

    caminho_destino = 'informar a rota aqui' 

    # Chamar a função principal ai de cima para executar a separação de cada aba em arquivos dataframe
    processador = ProcessadorExcel.ProcessadorExcel(caminho_pasta=caminho_pasta, caminho_destino=caminho_destino)

    # Tratativa e separação dos arquivos. Objetivo aqui é saber se deu certo.
    try:
        arquivos_excel = processador.carregar_abas()
        for nome_aba, df in arquivos_excel.items():
            #print(f"Manipulando a aba: {nome_aba}")
            manipulador = ManipuladorDataFrameAlocacaoComercial.ManipuladorDataFrame(df)
            #manipulador.exibir() 
    except FileNotFoundError as e:
        print(e)

    banco = BancoDeDadosAlocacaoComercial.BancoDeDados(dataframe='',nome_tabela='nome_aqui')

    dataframe = pd.DataFrame(df)
    manipulador = ManipuladorDataFrameAlocacaoComercial.ManipuladorDataFrame(dataframe)

    # No seu fluxo principal
    # Passo 1: Manipular dados
    manipulador.manipular_linha_negocio(banco)
    manipulador.manipular_produtores(banco)

    # Passo 2: Processar dados
    manipulador.processar()

    # Passo 3: Validar IDs
    df_final_grade = manipulador.grade_comercial
    df_final_grade_validado = manipulador.validar_ids(banco, df_final_grade)

    # Inserir coluna com data e hora atual para mostrar o momento da atualizaação
    df_final_grade_validado['DATA_ATUALIZACAO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    df_final_grade_validado = pd.DataFrame(df_final_grade_validado)

    # Montar a lista de queries conforme você já fez
    df_final_grade_validado = pd.DataFrame(df_final_grade_validado)
    lista_valores_final = df_final_grade_validado.values.tolist()

    querys_full = []
    for linha in lista_valores_final:
        dados_query = ', '.join([f"'{x}'" for x in linha])
        query = f"INSERT INTO GRADE_COMERCIAL VALUES ({dados_query})"
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
    banco = BancoDeDadosAlocacaoComercial.BancoDeDados(dataframe=df_final_grade_validado, nome_tabela='nome_aqui')
    banco._criar
    banco._truncar
    print("Iniciar processo de inserir os dados...")
    lista_execucao_paralela = Parallel(n_jobs=5)(delayed(banco._inserir_dados)(i) for i in pacotes_importacao)
    print("Dados inseridos com sucesso!")

    df_retorno = pd.DataFrame(lista_execucao_paralela)
    #df_retorno[df_retorno=='ERRO']

    # Chamar a função para enviar e-mail com esses casos

    # Limpar a pasta no final para não deixar arquivos.
    caminho_destino = 'informar a rota aqui' 

    limpar = ProcessadorExcel.ProcessadorExcel(caminho_pasta='', caminho_destino=caminho_destino)
    limpar.limpar_pasta_destino()

    print("Processo para alocação comercial foi finalizado!")

