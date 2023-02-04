from datetime import datetime, timedelta
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.baseoperator import chain # A function that sets sequential dependencies between tasks including lists of tasks.
#from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule # Used to change how an Operator is triggered
from airflow.utils.task_group import TaskGroup
from airflow.operators.oracle_operator import OracleOperator
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.providers.oracle.hooks.oracle import OracleHook
#from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.models.baseoperator import BaseOperator
#from minio.commonconfig import REPLACE, CopySource
from airflow.models import Variable
from minio import Minio
from os import getenv
from io import BytesIO
import pandas as pd
import gc


#["Variáveis AIRFLOW"]
MINIO = Variable.get('MINIO_URL')
ACESS_KEY = Variable.get('MINIO_ACESS_KEY')
SECRET_ACCESS = Variable.get('MINIO_SECRET_ACCESS')
MINIO_REGION = Variable.get('MINIO_REGION')
clientMinio = Minio(MINIO,ACESS_KEY,SECRET_ACCESS, secure=False)
AMBIENTE = Variable.get('AMBIENTE')

CURATED = getenv("CURATED","airflowcurated" if AMBIENTE == 'PROD' else 'airflowcurated-dev')
LANDING = getenv("LANDING","airflowlanding" if AMBIENTE == 'PROD' else 'airflowlanding-dev')
PROCESSING = getenv("PROCESSING","airflowprocess" if AMBIENTE == 'PROD' else 'airflowprocess-dev')
DATA_LAKE_VUON = getenv("DATA_LAKE_VUON","data-lake-vuon" if AMBIENTE == 'PROD' else "data-lake-vuon-dev" )

past = 'crm_vuon/'
n_arquivo = ('dados_transacionais.parquet')

class Processing(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        #self.tb = tb

    def listaMinio (bucket, past):
        objetos = []
        objects = clientMinio.list_objects(bucket,past)
        for obj in objects:
            objetos.append(obj.object_name.replace(past,""))
        #print(dataSets)
        return (objetos)

    def execute(self, context):

        #lst = [x + '.parquet' for x in list(tasks.keys())] # LISTA DOS ARQUIVOS GERADOS
        #print(f'LIST: {lst}')

        #dataSets = Processing.listaMinio(bucket = DATA_LAKE_VUON,past = past) # LISTA DOS ARQUIVOS GERADOS
        # dataSets = ['t_anuidade_produto.parquet', 't_cliente.parquet', 't_conta.parquet', 't_fat_limite.parquet',
        #     't_fatura.parquet', 't_pagamento_fatura.parquet', 't_parcelamento_fatura.parquet',
        #     't_seguros.parquet', 't_venda.parquet', 'ult_fat_emitida.parquet']
        #print(f'DATASETS: {dataSets}')

        #CRIAÇÃO DE DATAFRAMES A PARTIR DE AQUIVOS PARQUET DO MINIO
        df_t_anuidade_produto = pd.DataFrame(pd.read_parquet(BytesIO(clientMinio.get_object(DATA_LAKE_VUON,past+'t_anuidade_produto'+'.parquet').data)))
        df_t_cliente  = pd.DataFrame(pd.read_parquet(BytesIO(clientMinio.get_object(DATA_LAKE_VUON,past+'t_cliente'+'.parquet').data)))
        df_t_conta = pd.DataFrame(pd.read_parquet(BytesIO(clientMinio.get_object(DATA_LAKE_VUON,past+'t_conta'+'.parquet').data)))
        df_t_fat_limite =pd.DataFrame(pd.read_parquet(BytesIO(clientMinio.get_object(DATA_LAKE_VUON,past+'t_fat_limite'+'.parquet').data)))
        df_t_fatura = pd.DataFrame(pd.read_parquet(BytesIO(clientMinio.get_object(DATA_LAKE_VUON,past+'t_fatura'+'.parquet').data)))
        df_t_pagamento_fatura = pd.DataFrame(pd.read_parquet(BytesIO(clientMinio.get_object(DATA_LAKE_VUON,past+'t_pagamento_fatura'+'.parquet').data)))
        df_t_parcelamento_fatura = pd.DataFrame(pd.read_parquet(BytesIO(clientMinio.get_object(DATA_LAKE_VUON,past+'t_parcelamento_fatura'+'.parquet').data)))
        df_t_seguros = pd.DataFrame(pd.read_parquet(BytesIO(clientMinio.get_object(DATA_LAKE_VUON,past+'t_seguros'+'.parquet').data)))
        df_t_venda = pd.DataFrame(pd.read_parquet(BytesIO(clientMinio.get_object(DATA_LAKE_VUON,past+'t_venda'+'.parquet').data)))
        #df_ult_fat_emitida = pd.DataFrame(pd.read_parquet(BytesIO(clientMinio.get_object(DATA_LAKE_VUON,past+'ult_fat_emitida'+'.parquet').data)))

        #DELIMITAÇÃO DOS DATAFRAMES
        df_t_anuidade_produto = df_t_anuidade_produto[['ds_produto', 'id_produto', 'vl_faturamento_minimo']]
        df_t_cliente  = df_t_cliente [['id_conta', 'dt_cadastro']]
        df_t_conta = df_t_conta[['id_conta', 'nu_cpf_cnpj', 'tx_status_conta', 'tx_loja_origem', 'tx_status_cartao', 'tx_motivo_bloqueio',
                                'id_produto', 'tx_descricao_produto', 'qtd_dias_atraso', 'vl_absoluto_limite', 'vl_fatura_ativa']]
        df_t_fat_limite = df_t_fat_limite[['id_conta', 'vlr_absoluto_limite', 'vlr_limite_utilizado', 'vlr_limite_disponivel']]
        df_t_fatura = df_t_fatura[['id_fatura', 'id_conta', 'dt_vencimento_fatura', 'fl_ativa', 'fl_servico_sms', 'vlr_fatura']]
        df_t_pagamento_fatura = df_t_pagamento_fatura[['id_fatura', 'id_conta', 'dt_pagamento_ult_fatura', 'vlr_pago']]
        df_t_parcelamento_fatura = df_t_parcelamento_fatura[['id_conta', 'fl_status']]
        df_t_seguros = df_t_seguros[['cpf', 'id_produto']]
        df_t_venda = df_t_venda[['id_conta', 'dt_ultima_compra', 'dt_primeira_compra']]
        #df_ult_fat_emitida = df_ult_fat_emitida[['id_conta', 'id_fatura']]
        
        #Função de processamento
        df_t_parcelamento_fatura = df_t_parcelamento_fatura.drop_duplicates(keep="last")
        df_pagamento_fatura = df_t_pagamento_fatura[["id_fatura","id_conta","vlr_pago","dt_pagamento_ult_fatura"]]
        df_pagamento_fatura["vlr_pago"]= df_pagamento_fatura.groupby("id_fatura")["vlr_pago"].transform('sum')#transform(lambda x: x.sum())
        df_pagamento_fatura["dt_pagamento_ult_fatura"]= df_pagamento_fatura.groupby("id_fatura")["dt_pagamento_ult_fatura"].transform('max')#transform(lambda y: y.max())
        df_pagamento_fatura = df_pagamento_fatura[["id_fatura","id_conta","vlr_pago","dt_pagamento_ult_fatura"]]
        df_pagamento_fatura = df_pagamento_fatura.drop_duplicates()
        #df = df_t_fatura.merge(df_ult_fat_emitida , left_on=["id_fatura","id_conta"], right_on = ["id_fatura","id_conta"], how = 'left')
        df = df_t_fatura.merge(df_pagamento_fatura, left_on=["id_fatura","id_conta"], right_on = ["id_fatura","id_conta"], how = 'left')
        df = df.drop_duplicates()
        df = df.merge(df_t_conta, left_on="id_conta", right_on="id_conta", how = "inner")
        df = df.merge(df_t_anuidade_produto[["id_produto","vl_faturamento_minimo"]],left_on="id_produto", right_on="id_produto", how = "inner")
        df = df.merge(df_t_cliente, left_on = 'id_conta', right_on = 'id_conta', how = 'left')
        df = df.drop_duplicates()
        df = df.drop(["id_fatura"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_venda, left_on = 'id_conta', right_on = 'id_conta', how = 'left')
        df = df.merge(df_t_fat_limite, left_on = 'id_conta', right_on = 'id_conta', how = 'left')
        
        del df_t_fatura
        #del df_ult_fat_emitida
        del df_pagamento_fatura
        del df_t_pagamento_fatura
        del df_t_conta
        del df_t_anuidade_produto
        del df_t_venda
        del df_t_cliente
        del df_t_fat_limite
        gc.collect()

        df['dt_cadastro'] = pd.to_datetime(df['dt_cadastro'], errors='coerce').dt.date #dt.date
        df['dt_ultima_compra'] = pd.to_datetime (df['dt_ultima_compra']) #dt.date
        df['dt_primeira_compra'] = pd.to_datetime (df['dt_primeira_compra']) #dt.date
        print(df[['id_conta','dt_primeira_compra','dt_ultima_compra']][df.nu_cpf_cnpj=='09679295982'])
        df['dt_vencimento_fatura'] = pd.to_datetime(df['dt_vencimento_fatura'], errors='coerce').dt.date
        df['dt_pagamento_ult_fatura'] = pd.to_datetime(df['dt_pagamento_ult_fatura'], errors='ignore').dt.date
        df['vlr_limite_disponivel'] = df['vlr_limite_disponivel'].astype(float)
        df['vlr_absoluto_limite'] =  df['vlr_absoluto_limite'].astype(float)
        df['vl_absoluto_limite'] =  df['vl_absoluto_limite'].astype(float)
        df['vlr_limite_utilizado'] = df['vlr_limite_utilizado'].astype(float)
        df['vlr_limite_disponivel'] = [x if (pd.notnull(x) == True) else 0 for x in df['vlr_limite_disponivel']]
        #df['vlr_absoluto_limite'] =  np.where(df['vlr_absoluto_limite'].isnull(),df['vl_absoluto_limite'], df['vlr_absoluto_limite'])
        df['vlr_absoluto_limite'] =  df['vlr_absoluto_limite'].fillna(df['vl_absoluto_limite'])
        df['vlr_limite_utilizado'] = [x if (pd.notnull(x) == True) else 0 for x in df['vlr_limite_utilizado']]
        #df["vlr_limite_disponivel"]= pd.to_numeric(df.vlr_limite_disponivel, downcast="integer").round(2)
        #df["vlr_absoluto_limite"]= pd.to_numeric(df.vlr_absoluto_limite, downcast="integer").round(2)
        #df["vlr_limite_utilizado"]= pd.to_numeric(df.vlr_limite_utilizado, downcast="integer").round(2)
        df["vlr_fatura"]= pd.to_numeric(df.vlr_fatura, downcast="integer")
        df['fl_servico_sms'] = [1 if x == "S" else 0 for x in df["fl_servico_sms"]]
        df['fl_fatura_ativa_mes'] = [1 if x == "S" else 0 for x in df["fl_ativa"]]
        df = df.drop(["fl_ativa"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_parcelamento_fatura [['id_conta','fl_status']],left_on = 'id_conta', right_on = 'id_conta', how = 'left')
        df['fl_possui_parcelamento_fatura'] = [1 if x == True else 0 for x in pd.notnull(df["fl_status"])]
        df = df.drop(["fl_status"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_seguros['cpf'], left_on = 'nu_cpf_cnpj', right_on = 'cpf', how = 'left')
        df['fl_possui_seguros'] = [1 if x == True else 0 for x in pd.notnull(df["cpf"])]
        df = df.drop(["cpf"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_seguros[(df_t_seguros['id_produto'] == 3)]['cpf'], left_on = 'nu_cpf_cnpj', right_on = 'cpf', how = 'left')
        df['fl_possui_odonto'] = [1 if x == True else 0 for x in pd.notnull(df["cpf"])]
        df = df.drop(["cpf"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_seguros[df_t_seguros['id_produto'].isin([1,2])]['cpf'], left_on = 'nu_cpf_cnpj', right_on = 'cpf', how = 'left')
        df = df.drop_duplicates()
        df['fl_possui_fatura_garantida'] = [1 if x == True else 0 for x in pd.notnull(df["cpf"])]
        df = df.drop(["cpf"], axis=1) #=> EXCLUINDO COLUNA
        df.rename(columns={'nu_cpf_cnpj': 'id_pessoa'}, inplace=True)
        #df['id_pessoa']= [hashlib.md5(val.encode('utf-8')).hexdigest().upper() for val in df["nu_cpf_cnpj"]]
        #df = df.drop(["nu_cpf_cnpj"], axis=1) #=> EXCLUINDO COLUNA
        df = df[['id_pessoa','tx_status_cartao','tx_motivo_bloqueio', 'vlr_limite_utilizado','vlr_absoluto_limite', 'fl_possui_seguros',
                'fl_possui_odonto', 'fl_possui_parcelamento_fatura','fl_possui_fatura_garantida','dt_ultima_compra','dt_primeira_compra',
                'dt_cadastro','qtd_dias_atraso','tx_loja_origem','vlr_fatura','vlr_pago', 'tx_status_conta','id_produto','tx_descricao_produto',
                'id_conta','dt_vencimento_fatura','fl_servico_sms','fl_fatura_ativa_mes', 'dt_pagamento_ult_fatura',
                'vlr_limite_disponivel']]#,'qtd_dias_inativo', 'tx_loja_compra','tx_bandeira_cartao',
        n_arquivo = ('dados_transacionais.parquet')

        df = df.to_parquet(n_arquivo, compression='gzip') #Converte para parquet
        clientMinio.remove_object(PROCESSING, (past + n_arquivo))
        clientMinio.fput_object(PROCESSING, (past + n_arquivo), n_arquivo)
        

class InsereOracle(BaseOperator):
    def __init__(self, tb = str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tb = tb

    def execute(self, context):

        path = clientMinio.get_object(PROCESSING,past+n_arquivo)
        ds = pd.read_parquet(BytesIO(path.data))
        #print (ds.head())
        df = pd.DataFrame(ds)

        #df = df[(df.index > 709357)&(df.index < 709370)] #EXCLUIR

        rows = [tuple(x) for x in df.values]
        #print(df.head(12))

        if len(df) < 1:
            print('Não há novos registros')
        else:
            #print(df.head())
        
            columns = list(df.columns) #Os nomes de colunas do dataframe devem ser iguais aos da tabela de destino
            #print(f'Total de Colunas Dataframe : {len(columns)}')

            #print(df.columns)

            conn_crm = OracleHook(oracle_conn_id = "datawarehouse")
            conn_crm.bulk_insert_rows(table = "crm.stage_pessoa_cartao", 
            #conn_crm.bulk_insert_rows(table = "WANDERLEY_GONCALVES.Gpt_Integra_Cliente_Fila",
                                        rows = rows,
                                        target_fields = columns,
                                        commit_every = 10000)