class ProcessadorExcel:

    def __init__(self, caminho_pasta):
        self.caminho_pasta = caminho_pasta
        self.dataframes = {}

    def encontrar_arquivo_excel(self):
        for nome_arquivo in os.listdir(self.caminho_pasta):
            if nome_arquivo.endswith('.xlsx') or nome_arquivo.endswith('.xls'):
                return os.path.join(self.caminho_pasta, nome_arquivo)
        return None

    def carregar_abas(self):
        arquivo_excel = self.encontrar_arquivo_excel()
        if arquivo_excel:
            xls = pd.ExcelFile(arquivo_excel)
            for nome_aba in xls.sheet_names:
                self.dataframes[nome_aba] = pd.read_excel(xls, sheet_name=nome_aba)
            return self.dataframes
        else:
            raise FileNotFoundError("Nenhum arquivo Excel encontrado na pasta especificada. Avaliar se não mudou algo")
