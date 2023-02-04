from datetime import datetime, timedelta
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.baseoperator import chain # A function that sets sequential dependencies between tasks including lists of tasks.
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule # Used to change how an Operator is triggered
from airflow.utils.task_group import TaskGroup
from airflow.operators.oracle_operator import OracleOperator
from crm.crm_vuon_dados_transacionais_functions import Processing, InsereOracle
from airflow.sensors.external_task_sensor import ExternalTaskSensor

#[START default_dag]
default_args = {
        "owner": "crm",
        "depends_on_post" : False, #se depende de post anterior
        #"email":,
        #"email_on_failure": False,
        #"email_on_retries": 1,
        #"execution_timeout": timedelta(minutes=8),
        #"timeout": 36000,
        "retries": 5,  # If a task fails, it will retry 1 times
        "retry_delay": timedelta(minutes=1) #rodar novamente em caso de falha
    }

@dag(
    start_date=datetime(2022, 11, 1),
    max_active_runs=1,
    schedule_interval= "35 12 * * *",
    #schedule_interval="@daily",
    #schedule_interval= 1 * * * *,
    default_view="graph",
    catchup=False,
    default_args = default_args,
    tags=["crm", "vuon"], # If set, this tag is shown in the DAG view of the Airflow UI
)

def crm_vuon_dados_transacionais():

    init = DummyOperator(task_id="Init")
    start_DAG_sensor=ExternalTaskSensor(external_dag_id = 'crm_vuon_carga_data_lake', check_existence=True, task_id='start_DAG_sensor')
    with TaskGroup(group_id='Transform') as Transform:
        Processing_df = Processing(task_id = "Processing")#, tb = "t_cliente_gold")
        chain(Processing_df)
    with TaskGroup(group_id='Load') as Load:
        truncate_stg_pessoa_cartao= OracleOperator(task_id="truncate_stg_pessoa_cartao", sql = "truncate table crm.stage_pessoa_cartao", oracle_conn_id = "datawarehouse", autocommit = True)
        Insere_Oracle = InsereOracle(task_id = "Insere_Oracle", tb = 't_cliente_gold')
        chain(truncate_stg_pessoa_cartao, Insere_Oracle)
    finish = DummyOperator(task_id="Finish", trigger_rule=TriggerRule.NONE_FAILED)

    chain (init, start_DAG_sensor, Transform, Load, finish)
dag = crm_vuon_dados_transacionais()