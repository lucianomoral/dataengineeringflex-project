from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from tasks_definition import pushStockPricesIntoRedshiftDB, send_email

default_args={
    'owner': 'Luciano',
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    default_args=default_args,
    dag_id='TickerPricesLoad',
    description= 'Load companies data',
    start_date=datetime(2023,10,18,2),
    schedule_interval='@daily'
    ) as dag:
    task1= PythonOperator(
        task_id='load_companies_data',
        python_callable= pushStockPricesIntoRedshiftDB,
    )

    task2= PythonOperator(
        task_id='send_email_with_outcome',
        python_callable= send_email,
    )

    task1 >> task2