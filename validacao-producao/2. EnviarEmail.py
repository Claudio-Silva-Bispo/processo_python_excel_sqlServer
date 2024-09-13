import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import Autenticacoes


def enviar_email(assunto, destinario, copias, corpo):

    # Coletar a autenticação para acessar o servidor.
    dados =  Autenticacoes.DB_CONFIG.get('email')

    # Extrair os valores das chaves que precisamos.
    smtp_server = dados.get('smtp_server')
    port = dados.get('port')
    email_user = dados.get('email_user')
    email_password = dados.get('email_password')

    # Realizar a conexão
    server = smtplib.SMTP(smtp_server, port)
    server.starttls()  # Inicia a conexão TLS
    server.ehlo() 
    server.login(email_user, email_password)

    data_atual = datetime.datetime.now().strftime('%d/%m/%Y %X')

    message = MIMEMultipart()
    message["Subject"] = assunto
    message["From"] = email_user
    message["To"] = destinario
    message["CC"] = copias
    message['Bcc'] = ''

    # Corpo do e-mail
    message.attach(MIMEText(corpo, 'plain'))

    server.send_message(message)
    server.quit()


assunto = 'Erro no processo de validação'
destinatario = ''
copias = ''
corpo = "Falha no processo de validação das colunas."
EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo)
