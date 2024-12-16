from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from modules.etl import *


from datetime import datetime

def fun_extract_top_countries(**kwargs):
    extract_transform_top_countries()

def fun_load_top_countries(*kwargs):
    load_top_countries()


with DAG(
    dag_id='project3_top_countries',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:
    start_task=EmptyOperator(
        task_id='start'
    )

    op_extract_transform_top_countries=PythonOperator(
        task_id='extract_transform_top_countries',
        python_callable=fun_extract_top_countries
    )

    op_load_top_countries=PythonOperator(
        task_id='load_top_countries',
        python_callable=fun_load_top_countries
    )

    end_task=EmptyOperator(
        task_id='end'
    )


start_task >> op_extract_transform_top_countries >> op_load_top_countries >> end_task