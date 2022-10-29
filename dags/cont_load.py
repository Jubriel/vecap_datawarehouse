# from asyncio import tasks
# from sched import scheduler
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine
from airflow.hooks.base_hook import BaseHook

from datetime import datetime

from ETL_c import *

# POSTGRRES_CONN_ID = "vecap_source"
# POSTGRRES_CONN_ID2 = "vecap_target"

src = BaseHook.get_connection('vecap_source')
tgt = BaseHook.get_connection('vecap_target')

# Connectiion engines to source and target databases
source = create_engine(f"postgresql+psycopg2://{src.login}:{src.password}@{src.host}:{src.port}/{src.schema}")
target = create_engine(f"postgresql+psycopg2://{tgt.login}:{tgt.password}@{tgt.host}:{tgt.port}/{tgt.schema}")


arg = {
    'owner':'airflow',
    'start_date' : datetime(2022, 10, 22),
    # 'retries': None,
    'email':['admin@vecap.com'],
    'email_on_failure':False,
    'email_on_retry':False
}

etl_dag = DAG(
    dag_id = 'vecap_etl',
    schedule_interval = '@daily',
    #catch_up = False,
    default_args=arg
)

# ETL starts

fam= PythonOperator(
    task_id = 'fam_dim',
    python_callable=fam_dim,
    op_kwargs= {'sch':'rccghge', 'source':source, 'target':target},
    dag = etl_dag
)
per_dim = PythonOperator(
    task_id = 'person_dim',
    python_callable=person_dim,
    op_kwargs= {'sch':'rccghge', 'source':source, 'target':target},
    dag = etl_dag
)
pro_dim = PythonOperator(
    task_id = 'process_dim',
    python_callable=process_dim,
    op_kwargs= {'sch':'rccghge', 'source':source, 'target':target},
    dag = etl_dag
)
per_f = PythonOperator(
    task_id = 'person_fact',
    python_callable=per_fact,
    op_kwargs= {'sch':'rccghge', 'target':target},
    dag = etl_dag
)
ch_f = PythonOperator(
    task_id = 'church_fact',
    python_callable = church_fact,
    op_kwargs= {'sch':'rccghge', 'target':target},
    dag = etl_dag
)

per_dim >> fam >> pro_dim >> [per_f, ch_f]
