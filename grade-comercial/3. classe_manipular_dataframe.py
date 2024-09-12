
# Para manipulação do arquivo em Excel
import pandas as pd

from itertools import product


# Import demais componentes
import BancoDeDadosAlocacaoComercial

class ManipuladorDataFrame:
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def exibir(self):
        print(self.dataframe)

    def manipular_linha_negocio(self, banco: BancoDeDadosAlocacaoComercial.BancoDeDados):
        # Consultar dados de linha de negócio
        query_linha_negocio = """
        SELECT
            idLinhaNegocio as ID_LINHA_NEGOCIO,
            LinhaNegocio as LINHA_NEGOCIO
        FROM tabela
        """
        self.linha_negocio = banco.consultar(query_linha_negocio, 'nome_database')
        self.linha_negocio['ID_LINHA_NEGOCIO'] = self.linha_negocio['ID_LINHA_NEGOCIO'].astype(str)

    def manipular_produtores(self, banco: BancoDeDadosAlocacaoComercial.BancoDeDados):
        # Consultar dados da tabela Produtor
        query_produtores = "SELECT * FROM nome_tabela"
        self.dim_produtores = banco.consultar(query_produtores, 'nome_database')

    def processar(self):
        # Ajustar as colunas e renomear
        self.dataframe = self.dataframe[['Colunas']]
        
        # Renomear as colunas
        self.dataframe = self.dataframe.rename(columns={
            'nome_original': 'novo_nome',
          
        })

        # Conversões e manipulações
        self.dataframe['LINHA_NEGOCIO'] = self.dataframe['LINHA_NEGOCIO'].str.upper()
        self.dataframe['FLAG_PRODUTOR_ESPECIALISTA'] = self.dataframe['ID_PRODUTOR_ESPECIALISTA'].notnull().astype(bool)
        self.dataframe.replace('-', None, inplace=True)

        # Processar DataFrame conforme sua lógica
        df_novo = self.dataframe.copy()
        df_cor = df_novo[df_novo.FLAG_PRODUTOR_ESPECIALISTA == False]
        df_esp = df_novo[df_novo.FLAG_PRODUTOR_ESPECIALISTA == True][['ID_PESSOA_CORRETOR', 'ID_PRODUTOR_ESPECIALISTA', 'LINHA_NEGOCIO']]
        
        # Manipulações adicionais
        df_esp['LINHA_NEGOCIO'] = df_esp['LINHA_NEGOCIO'].str.split(';')
        df_esp = df_esp.explode('LINHA_NEGOCIO').reset_index(drop=True)
        df_esp['LINHA_NEGOCIO'] = df_esp['LINHA_NEGOCIO'].str.strip()

        df_esp = df_esp.merge(self.linha_negocio, how='left', left_on='LINHA_NEGOCIO', right_on='LINHA_NEGOCIO')
        df_esp = df_esp[df_esp.LINHA_NEGOCIO.notnull()]

        # Filtragem e criação do DataFrame final
        lista_produtores = list(self.dim_produtores['id_pessoa'])
        df_esp = df_esp[df_esp['ID_PRODUTOR_ESPECIALISTA'].isin(lista_produtores)]

        # Criação do DataFrame cartesiano
        ids_linha = list(self.linha_negocio['ID_LINHA_NEGOCIO'])
        ids_corretores = list(df_novo['ID_PESSOA_CORRETOR'])
        cartesiano_corretores = list(product(ids_corretores, ids_linha))

        df_final = pd.DataFrame(cartesiano_corretores, columns=['ID_PESSOA_CORRETOR', 'ID_LINHA_NEGOCIO'])
        df_grade = df_final.merge(df_novo, how='left', on='ID_PESSOA_CORRETOR')

        # Função para verificar produtores
        def verificar_produtor(id: int):
            default = 12345678
            return id if id in lista_produtores else default

        df_grade['ID_PRODUTOR_PRINCIPAL_VALIDADO'] = df_grade['ID_PRODUTOR_PRINCIPAL'].apply(verificar_produtor)

        # Preparar o DataFrame final
        df_grade = df_grade[['nome_colunas']]

        df_final_grade = df_grade.merge(df_esp, how='left', on=['ID_PESSOA_CORRETOR', 'ID_LINHA_NEGOCIO'])
        df_final_grade[['ID_PRODUTOR_ESPECIALISTA', 'LINHA_NEGOCIO']] = df_final_grade[['ID_PRODUTOR_ESPECIALISTA', 'LINHA_NEGOCIO']].fillna('-')
        df_final_grade['ID_PRODUTOR'] = df_final_grade.apply(lambda linha: linha['ID_PRODUTOR_ESPECIALISTA'] if linha['LINHA_NEGOCIO'] != '-' else linha['ID_PRODUTOR_PRINCIPAL_VALIDADO'], axis=1)                                        

        # Armazenar o resultado final
        self.grade_comercial = df_final_grade

        # Verifique se a coluna 'ID_LINHA_NEGOCIO' está presente
        #print("Colunas de df_esp:", df_final_grade.columns)


    def validar_e_substituir_ids(self, base_recebida: pd.DataFrame, coluna_grade: str, coluna_base_recebida: str, id_default: str):
        self.dataframe[coluna_grade] = self.dataframe[coluna_grade].astype(str)
        base_recebida[coluna_base_recebida] = base_recebida[coluna_base_recebida].astype(str)

        self.dataframe[coluna_grade] = self.dataframe[coluna_grade].where(
            self.dataframe[coluna_grade].isin(base_recebida[coluna_base_recebida]),
            id_default
        )
    
    def validar_ids(self, banco: BancoDeDadosAlocacaoComercial.BancoDeDados, df_final_grade: pd.DataFrame):

        # Atualizar o DataFrame a ser validado
        self.dataframe = df_final_grade.copy()

        # Validação para cada tabela conforme seu código
        tabelas = [
            ("CANAL", "ID_CANAL", "ID_CANAL", 3), # Inserir demais validações aqui, só seguir este modelo
        ]

        for tabela, coluna_grade, coluna_base, id_default in tabelas:
            query = f"SELECT * FROM {tabela}"
            base_recebida = banco.consultar(query, 'gestao_comercial')
            self.validar_e_substituir_ids(base_recebida, coluna_grade, coluna_base, id_default)


        # Ajustar ID_ASSESSORIA e ID_LINHA_NEGOCIO
        if 'ID_ASSESSORIA' in self.dataframe.columns:
            self.dataframe['ID_ASSESSORIA'] = self.dataframe['ID_ASSESSORIA'].replace("-", "0").fillna("0")

        if 'ID_LINHA_NEGOCIO' in self.dataframe.columns:
            self.dataframe['ID_LINHA_NEGOCIO'] = self.dataframe['ID_LINHA_NEGOCIO'].replace("-", "0").fillna("0")

        if 'ID_PRODUTOR_ESPECIALISTA' in self.dataframe.columns:
            self.dataframe['ID_PRODUTOR_ESPECIALISTA'] = self.dataframe['ID_PRODUTOR_ESPECIALISTA'].replace("-", "12345678").fillna("1234578")

        if 'LINHA_NEGOCIO' in self.dataframe.columns:
            self.dataframe['LINHA_NEGOCIO'] = self.dataframe['LINHA_NEGOCIO'].replace("-", "0").fillna("0")

        # preciso confirmar que deu certo
        return self.dataframe 
