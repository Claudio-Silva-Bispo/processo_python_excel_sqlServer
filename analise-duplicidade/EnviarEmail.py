from email import encoders
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

import pandas as pd
import Autenticacoes


def enviar_email(assunto: str, destinatario: str, copias: str, corpo: str, corpo_html : str, caminho_anexo_xlsx=None, nome_arquivo_xlsx ='arquivo_erros.xlsx'):

    # Coletar a autenticação para acessar o servidor.
    dados =  Autenticacoes.DB_CONFIG.get('email')

    # Extrair os valores das chaves que precisamos.
    smtp_server = dados.get('smtp_server')
    port = dados.get('port')
    email_user = dados.get('email_user')
    email_password = dados.get('email_password')

    # Realizar a conexão
    server = smtplib.SMTP(smtp_server, port)
    server.starttls()
    server.ehlo() 
    server.login(email_user, email_password)

    
    # message = MIMEMultipart()
    # Aqui vai aceitar minha personalização em html
    message = MIMEMultipart('alternative')
    message["Subject"] = assunto
    message["From"] = email_user
    message["To"] = destinatario
    message["CC"] = copias
    message['Bcc'] = ''

    # Adicionar as duas partes do e-mail (texto e HTML)
    parte_texto = MIMEText(corpo, 'plain')
    parte_html = MIMEText(corpo_html, 'html')

    message.attach(parte_texto)
    message.attach(parte_html)

    if caminho_anexo_xlsx is not None:
        # Abrir o arquivo e ajustar para enviar no anexo.
        with open(caminho_anexo_xlsx, 'rb') as arquivo_anexo:
            anexo = MIMEApplication(arquivo_anexo.read(), _subtype='xlsx') # Trocar para excel
            anexo.add_header('content-disposition', 'attachment', filename = nome_arquivo_xlsx)
            message.attach(anexo)
            arquivo_anexo.close()

    server.send_message(message)
    server.quit()
