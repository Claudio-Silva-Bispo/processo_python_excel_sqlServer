# Estrutura comercial
import funcaoPrincipalEstruturaComercial

# Alocação da grade comercial
import funcaoPrincipalAlocacaoComercial

# Tirar os warnings desnecessários
import warnings

# Suprimir todos os warnings
warnings.filterwarnings("ignore")

# Demais bibliotecas
import pandas as pd
from joblib import Parallel, delayed
from datetime import datetime

funcaoPrincipalEstruturaComercial.funcao_principal()

funcaoPrincipalAlocacaoComercial.funcao_principal_alocacao_comercial()

print("Processo para montar a Grade e Alocação Comercial foi concluído com sucesso")
