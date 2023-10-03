from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'owner': 'Luciano',
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}

def load_companies_data():
    with open("Entregable3.py") as f:
        exec(f.read())

with DAG(
    default_args=default_args,
    dag_id='dag1',
    description= 'Cargar companies data',
    start_date=datetime(2023,9,1,2),
    schedule_interval='@daily'
    ) as dag:
    task1= PythonOperator(
        task_id='load_companies_data',
        python_callable= load_companies_data,
    )

    task1