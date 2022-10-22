# from asyncio import tasks
# from sched import scheduler
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine
from airflow.hooks.base_hook import BaseHook

from datetime import datetime

from ETL import *

# POSTGRRES_CONN_ID = "vecap_source"
# POSTGRRES_CONN_ID2 = "vecap_target"

src = BaseHook.get_connection('vecap_source')
tgt = BaseHook.get_connection('vecap_target')

# Connectiion engines to source and target databases
source = create_engine(f"postgresql+psycopg2://{src.login}:{src.password}@{src.host}:{src.port}/{src.schema}")
target = create_engine(f"postgresql+psycopg2://{tgt.login}:{tgt.password}@{tgt.host}:{tgt.port}/{tgt.schema}")


arg = {
    'owner':'airflow',
    'start_date' : datetime(2022, 9, 8),
    # 'retries': None,
    'email':['admin@vecap.com'],
    'email_on_failure':False,
    'email_on_retry':False
}

etl_dag = DAG(
    dag_id = 'vecap_etl',
    schedule_interval = '@once',
    #catch_up = False,
    default_args=arg
)


dat = PostgresOperator(
    task_id = 'date_dim',
    postgres_conn_id='vecap_target',
    sql = """INSERT INTO {{ params.sch }}.date_dim
            SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_key,
                datum AS date_actual,
                EXTRACT(EPOCH FROM datum) AS epoch,
                TO_CHAR(datum, 'fmDDth') AS day_suffix,
                TO_CHAR(datum, 'TMDay') AS day_name,
                EXTRACT(ISODOW FROM datum) AS day_of_week,
                EXTRACT(DAY FROM datum) AS day_of_month,
                datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
                EXTRACT(DOY FROM datum) AS day_of_year,
                TO_CHAR(datum, 'W')::INT AS week_of_month,
                EXTRACT(WEEK FROM datum) AS week_of_year,
                EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
                EXTRACT(MONTH FROM datum) AS month_actual,
                TO_CHAR(datum, 'TMMonth') AS month_name,
                TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
                EXTRACT(QUARTER FROM datum) AS quarter_actual,
                CASE
                    WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
                    WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
                    WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
                    WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
                    END AS quarter_name,
                EXTRACT(YEAR FROM datum) AS year_actual,
                datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
                datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
                datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
                (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
                DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
                (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
                TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
                TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
                TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
                TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
                CASE
                    WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
                    ELSE FALSE
                    END AS weekend_indr
            FROM (SELECT '2020-01-01'::DATE + SEQUENCE.DAY AS datum
                FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
                GROUP BY SEQUENCE.DAY) DQ
            ORDER BY 1;
            COMMIT;""",
    params = {"sch": 'rccghge'},
    dag = etl_dag
)

fam = PythonOperator(
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

dat >> fam >> per_dim >> [per_f, ch_f]
