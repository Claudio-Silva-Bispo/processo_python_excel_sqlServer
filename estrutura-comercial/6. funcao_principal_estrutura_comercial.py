
from datetime import datetime
import sys
import BancoDadosEstruturaComercial
import ProcessadorExcel
import ManipuladorDataFrameEstruturaComercial

import pandas as pd

import EnviarEmail
import script_html

# Passar aqui caminho onde os arquivos ficarão armazenados.
caminho_pasta = fr''
caminho_destino = fr''

processador = ProcessadorExcel.ProcessadorExcel(caminho_pasta, caminho_destino)

# Tratativa e separação dos arquivos. Objetivo aqui é saber se deu certo.
try:
    dataframes = processador.carregar_abas()
    for nome_aba, df in dataframes.items():
        #print(f"Manipulando a aba: {nome_aba}")
        manipulador = ManipuladorDataFrameEstruturaComercial.ManipuladorDataFrame(df)
except FileNotFoundError as e:

        assunto = 'Verificar processo de atualização da ESTRUTURA COMERCIAL.'
        destinatario =script_html.suporte_email.get('dados_falha_estrutura').get('destinatario')
        copias = script_html.suporte_email.get('dados_falha_estrutura').get('copias')
        corpo = 'Processo foi interrompido na etapa inicial de procurar o arquivo.'
        corpo_html = script_html.html_falha

        EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html, arquivo_anexo='',caminho_anexo_xlsx='', nome_arquivo_xlsx='')

        sys.exit(f"Não foi possível encontrar o arquivo na pasta {caminho_pasta}")

def funcao_principal_estrutura():

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
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        if dataframe is not None: 
            dataframe['DATA_ATUALIZACAO'] = agora
            nome_conexao = "gestao_comercial"
            inserir_dados = BancoDadosEstruturaComercial.BancoDados(dataframe, nome_tabela, nome_conexao)
            inserir_dados.inserir()
            print(f"Dados inseridos na tabela: {nome_tabela}")
        else:
            print(f"DataFrame para a tabela {nome_tabela} não encontrado.")
            assunto = f'Verificar processo de atualização da ESTRUTURA COMERCIAL. Não foi possível inserir as informações no banco/n. DataFrame para a tabela {nome_tabela} não encontrado.'
            destinatario =script_html.suporte_email.get('dados_falha_estrutura').get('destinatario')
            copias = script_html.suporte_email.get('dados_falha_estrutura').get('copias')
            corpo = 'Processo foi interrompido na etapa inicial de procurar o arquivo.'
            corpo_html = script_html.html_falha

            EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html, arquivo_anexo='',caminho_anexo_xlsx='', nome_arquivo_xlsx='')

    # Enviar somente a base consolidada
    base_consolidada = dataframes.get('BASE_CONSOLIDADA')
    base_consolidada
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    base_consolidada['DATA_ATUALIZACAO'] = agora

    if any(base_consolidada.columns.str.startswith('Unnamed')):
        base_consolidada = base_consolidada.loc[:, ~base_consolidada.columns.str.startswith('Unnamed')]

    try:
        inserir_dados = BancoDadosEstruturaComercial.BancoDados(dataframe=base_consolidada, nome_tabela='METAS', nome_conexao='nome_aqui')
        inserir_dados.inserir()
    except Exception as e:
        print(f"Erro ao inserir dados na tabela METAS: {e}")

    limpar = ProcessadorExcel.ProcessadorExcel(caminho_pasta='', caminho_destino=caminho_destino)
    limpar.limpar_pasta_destino()
    print("Processo para criar a estrutura da Gestão Comercial finalizou.")

    assunto = script_html.suporte_email.get('dados_sucesso_estrutura').get('assunto')
    destinatario =script_html.suporte_email.get('dados_sucesso_estrutura').get('destinatario')
    copias = script_html.suporte_email.get('dados_sucesso_estrutura').get('copias')
    corpo = 'Processo para criar a ESTRUTURA da Gestão Comercial finalizou.'
    corpo_html = script_html.html_sucesso_estrutura

    EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, corpo_html=corpo_html,caminho_anexo_xlsx=None, nome_arquivo_xlsx='')

    print("E-mail enviado para todos sinalizando sobre a finalização do processo.")

funcao_principal_estrutura()
