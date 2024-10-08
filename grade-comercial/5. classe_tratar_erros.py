# Manipulação do e-mail
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import autenticacoes

class GerenciadorErros:
    def enviar_email_erro(assunto, destinatarios, copia, corpo):
        # Coletar a autenticação para acessar o servidor.
        dados = autenticacoes.DB_CONFIG.get('email')

        smtp_server = dados.get('smtp_server')
        smtp_port = dados.get('port')
        email_user = dados.get('email_user')
        email_password = dados.get('email_password')
        remetente = 'bi@ezzeseguros.com.br'

        # Criar a mensagem
        mensagem = MIMEMultipart()
        mensagem['From'] = remetente
        mensagem['To'] = ', '.join(destinatarios)
        mensagem['Cc'] = ', '.join(copia)
        mensagem['Subject'] = assunto
        mensagem.attach(MIMEText(corpo, 'plain'))

        try:
            with smtplib.SMTP(smtp_server, smtp_port) as servidor:
                servidor.starttls() 
                servidor.login(email_user, email_password) 
                servidor.sendmail(remetente, destinatarios + copia, mensagem.as_string())
                print("E-mail enviado com sucesso.")
        except Exception as e:
            print(f"Erro ao enviar e-mail: {e}")

