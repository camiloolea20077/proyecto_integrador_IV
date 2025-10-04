from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys, os

BASE_DIR = "/opt/airflow/src"
sys.path.append(BASE_DIR)

from extract import extract
from transform import run_queries
from load import load

default_args = {
    "owner": "camilo",
    "start_date": datetime(2025, 10, 1),
    "retries": 1,
}

with DAG(
    "elt_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=run_queries,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
