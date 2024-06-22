from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from data_cleaning_etl import get_data_etl



default_args = {
    'owner': 'Abhishek',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 1),
    'email': ['test@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    'data_fetch_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)

folder = 'C:/Users/abhie/OneDrive - Northeastern University/PatternDetection/Out-of-Pattern-Detection/HTTPRequests/demo'
run_etl = PythonOperator(
    task_id='complete_data_cleaning_etl',
    python_callable=get_data_etl,
    op_kwargs={'folder': folder},
    dag=dag, 
)

run_etl