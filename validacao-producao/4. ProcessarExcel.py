import os
import shutil
import psutil
import time
import pandas as pd
from typing import List
import EnviarEmail

class ProcessadorExcel:
    def __init__(self, caminho_pasta_origem: str, caminho_destino: str):
        self.caminho_pasta_origem = caminho_pasta_origem
        self.caminho_destino = caminho_destino

    def procurar_arquivos(self) -> List[str]:
        arquivos = [
            os.path.join(self.caminho_pasta_origem, arquivo)
            for arquivo in os.listdir(self.caminho_pasta_origem)
            if arquivo.endswith('.xlsx') or arquivo.endswith('.xls')
        ]
        
        if not arquivos:
            print("Nenhum arquivo Excel encontrado na pasta!")
            assunto = 'Processo Base Fria - Erro para encontrar o arquivo'
            corpo = fr'O primeiro processo falhou ao encontrar os arquivos na pasta de origem: {self.caminho_pasta_origem}'
            destinatario = ''
            copias = ''
            EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, arquivo_anexo='')
        
        return arquivos

    def copiar_arquivos(self, arquivos: List[str]) -> None:
        if not os.path.exists(self.caminho_destino):
            os.makedirs(self.caminho_destino)
        for arquivo in arquivos:
            try:
                shutil.copy(arquivo, self.caminho_destino)
                print(f"Arquivo {os.path.basename(arquivo)} copiado com sucesso para {self.caminho_destino}.")
            except Exception as e:
                print(f"Erro ao copiar {arquivo}: {e}. Enviar e-mail avisando que deu errado")
                assunto = 'Processo Base Fria - Erro para copiar os arquivos'
                corpo = fr'O Segundo processo falhou ao copiar os arquivos na pasta de destino: {self.caminho_destino}'
                destinatario = ''
                copias = ''
                EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, arquivo_anexo='')

    def ler_e_concatenar_arquivos(self) -> pd.DataFrame:
        arquivos_excel = self.procurar_arquivos()
        
        if not arquivos_excel:
            assunto = 'Processo Base Fria - Erro para gerar o dataframe'
            corpo = 'O Terceiro processo falhou ao encontrar os arquivos e compilar todos para gerar um único dataframe consolidado.'
            destinatario = ''
            copias = ''
            EnviarEmail.enviar_email(assunto=assunto, destinatario=destinatario, copias=copias, corpo=corpo, arquivo_anexo='')
            return pd.DataFrame()  # Retorna um DataFrame vazio se nenhum arquivo for encontrado e enviar um e-mail avisando que não deu certo
            
        lista_dfs = [pd.read_excel(arquivo) for arquivo in arquivos_excel]
        consolidado_total = pd.concat(lista_dfs, ignore_index=True)
        
        return consolidado_total

    def fechar_processos_excel(self):
        for proc in psutil.process_iter(attrs=['pid', 'name']):
            if proc.info['name'] == "EXCEL.EXE":
                print(f"Encerrando processo Excel com PID: {proc.info['pid']}")  # Debug
                proc.terminate()  # Usar terminate() é mais seguro

    def limpar_pasta_destino(self):
        self.fechar_processos_excel()
        time.sleep(5)  # Aguarda um pouco para garantir que o processo foi encerrado

        for arquivo in os.listdir(self.caminho_destino):
            arquivo_completo = os.path.join(self.caminho_destino, arquivo)
            if os.path.isfile(arquivo_completo):
                try:
                    os.remove(arquivo_completo)
                    print(f"Arquivo {arquivo_completo} removido com sucesso.")
                except PermissionError:
                    print(f"Erro ao remover {arquivo_completo}.")


