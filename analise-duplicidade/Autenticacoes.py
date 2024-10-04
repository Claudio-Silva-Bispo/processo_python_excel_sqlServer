# Configurações de conexão com o banco de dados SQL Server
DB_CONFIG = {
    "dw_producao" : {
    'driver': '{ODBC Driver 17 for SQL Server}', 
    'servidor': '',
    'banco': '',
    'usuario': '',
    'senha': ''},        

    "gestao_comercial" : {   
    'driver': '{ODBC Driver 17 for SQL Server}', 
    'servidor': 'EZZE-DB-03',
    'banco': '',
    'usuario': '',
    'senha': ''
    },

    # Realizar a conexão no servidor do Outlook
    "email": { 
    'smtp_server':'smtp-mail.outlook.com',
    'port': '587', 
    'email_user':'',
    'email_password':''
    },
}


