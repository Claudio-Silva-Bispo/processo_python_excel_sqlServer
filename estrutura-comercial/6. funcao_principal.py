
import BancoDadosEstruturaComercial
import ProcessadorExcel
import ManipuladorDataFrameEstruturaComercial

import pandas as pd

# Passar aqui caminho onde os arquivos ficarão armazenados.
caminho_pasta = fr'inserir caminho aqui'
caminho_destino = fr'inserir caminho aqui'

# Chamar a função principal ai de cima para executar a separação de cada aba em arquivos dataframe
processador = ProcessadorExcel.ProcessadorExcel(caminho_pasta, caminho_destino)

# Tratativa e separação dos arquivos. Objetivo aqui é saber se deu certo.
try:
    dataframes = processador.carregar_abas()
    for nome_aba, df in dataframes.items():
        #print(f"Manipulando a aba: {nome_aba}")
        manipulador = ManipuladorDataFrameEstruturaComercial.ManipuladorDataFrame(df)
except FileNotFoundError as e:
    print(e)

def funcao_principal():

    # Inserir os dados conforme a lista de tabelas e seus dataframes.
    tabelas_e_dataframes = [
        ('RESUMO', dataframes.get('RESUMO')),
        ('CANAL', dataframes.get('CANAL')),
        ('HEAD_CANAL', dataframes.get('HEAD_CANAL')),
        ('VERTICAL', dataframes.get('VERTICAL')),
        ('HEAD_VERTICAL', dataframes.get('HEAD_VERTICAL')),
        ('SUB_VERTICAL', dataframes.get('SUB_VERTICAL')),
        ('HEAD_SUB_VERTICAL', dataframes.get('HEAD_SUB_VERTICAL')),
        ('FILIAL', dataframes.get('FILIAL')),
        ('PRODUTOR', dataframes.get('PRODUTOR')),
        ('DEPARA_LINHA_NEGOCIO', dataframes.get('DEPARA_LINHA_NEGOCIO'))
    ]

    # Insira os dados conforme a lista de tabelas e seus dataframes.
    for nome_tabela, dataframe in tabelas_e_dataframes:
        if dataframe is not None: 
            nome_conexao = "gestao_comercial"
            inserir_dados = BancoDadosEstruturaComercial.BancoDados(dataframe, nome_tabela, nome_conexao)
            inserir_dados.inserir()
            print(f"Dados inseridos na tabela: {nome_tabela}")
        else:
            print(f"DataFrame para a tabela {nome_tabela} não encontrado.")

    # Enviar somente a base consolidada
    base_consolidada = dataframes.get('BASE_CONSOLIDADA')
    base_consolidada['DATA_BASE'] = pd.to_datetime(base_consolidada['DATA_BASE'], errors='coerce')
    inserir_dados = BancoDadosEstruturaComercial.BancoDados(dataframe, nome_tabela, nome_conexao)

    limpar = ProcessadorExcel.ProcessadorExcel(caminho_pasta='', caminho_destino=caminho_destino)
    limpar.limpar_pasta_destino()
    print("Processo para criar a estrutura da Gestão Comercial finalizou.")

funcao_principal()
print("Processo para criar a estrutura da Gestão Comercial finalizou.")
