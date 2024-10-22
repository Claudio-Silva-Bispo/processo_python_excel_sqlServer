import pandas as pd
from typing import List
import BancoDados
import EnviarEmail
import script_html
import sys

query = """

"""


banco = BancoDados.BancoDeDados(nome_tabela='nome_database', dataframe=None)

try:
    
    df = banco.consultar(nome_database='nome_database', query=query) 
    if df.empty:
        #print('O dataframe está vazio. Finalizando o processo pois não é necessário atuação.')
        destinatario =script_html.suporte_email.get('dados_sucesso').get('destinatario')
        copias = script_html.suporte_email.get('dados_sucesso').get('copias')
        corpo_html = script_html.html_sucesso
        EnviarEmail.enviar_email(assunto='ALOCAÇÃO COMERCIAL: Processo verificou se possui duplicidades!',destinatario=destinatario, copias=copias, corpo='', corpo_html=corpo_html,caminho_anexo_xlsx=None, nome_arquivo_xlsx='' )
        sys.exit()
    else:

        df.dropna()
        df.to_excel('arquivo_erros.xlsx', index=False)

        arquivo_erro = pd.read_excel('arquivo_erros.xlsx', index_col=False)

        caminho_anexo = 'arquivo_erros.xlsx'

        destinatario =script_html.suporte_email.get('dados_falha').get('destinatario')
        copias = script_html.suporte_email.get('dados_falha').get('copias')
        corpo_html = script_html.html_falha
        EnviarEmail.enviar_email(assunto='Urgente: Alocação comercial possui duplicidade',destinatario=destinatario, copias=copias, corpo='', corpo_html=corpo_html,caminho_anexo_xlsx=caminho_anexo, nome_arquivo_xlsx='arquivo_erros.xlsx' )
        sys.exit(f"Finalizado o processo com envio do e-mail para tratativa.")
except:
    print("Erro no processo.")
