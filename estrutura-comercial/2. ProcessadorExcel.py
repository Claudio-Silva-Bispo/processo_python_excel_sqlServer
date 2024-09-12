
# Para manipulação do arquivo em Excel
import pandas as pd
import os
import shutil

# Personalização das funções e documentar com tipos esperados e formatações necessárias que ajudem outros usuários.
from typing import Literal, List, Any

# Para encerrar o processo do excel uma vez que eu tiver o dataframe
import psutil
import time


class ProcessadorExcel:

    def __init__(self, caminho_pasta : Literal['colocar caminho aqui'], caminho_destino: str):
        self.caminho_pasta = caminho_pasta
        self.caminho_destino = caminho_destino
        self.dataframes = {}

    def encontrar_arquivo_excel(self):
        for nome_arquivo in os.listdir(self.caminho_pasta):
            if nome_arquivo.endswith('.xlsx') or nome_arquivo.endswith('.xls'):
                return os.path.join(self.caminho_pasta, nome_arquivo)
        return None
    
    def copiar_arquivo(self):
        arquivo_excel = self.encontrar_arquivo_excel()
        if arquivo_excel:
            # Copiar o arquivo para a pasta de destino
            shutil.copy(arquivo_excel, self.caminho_destino)
            return os.path.join(self.caminho_destino, os.path.basename(arquivo_excel))
        else:
            raise FileNotFoundError("Nenhum arquivo Excel encontrado na pasta!")

        
    def carregar_abas(self):
         # Usar o arquivo copiado e não o original
        arquivo_copiado = self.copiar_arquivo() 
        xls = pd.ExcelFile(arquivo_copiado)
        for nome_aba in xls.sheet_names:
            self.dataframes[nome_aba] = pd.read_excel(xls, sheet_name=nome_aba)

        xls.close()
        return self.dataframes
    
    def fechar_processos_excel(self):
        for proc in psutil.process_iter():
            if proc.name() == "EXCEL.EXE":
                proc.kill()
        
    def limpar_pasta_destino(self):
        self.fechar_processos_excel()
        time.sleep(5)

        for arquivo in os.listdir(self.caminho_destino):
            arquivo_completo = os.path.join(self.caminho_destino, arquivo)
            if os.path.isfile(arquivo_completo):
                # os.remove(arquivo_completo)
                try:
                    os.remove(arquivo_completo)
                    print(f"Arquivo {arquivo_completo} removido com sucesso.")
                except PermissionError:
                    print(f"Erro ao remover {arquivo_completo}.")
