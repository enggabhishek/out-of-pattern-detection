from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import  PythonOperator
import os
# from dotenv import load_dotenv
# load_dotenv()
def create_bar_graph():
    account_url=os.getenv("Data_Lake_URL")
    sas_url=os.getenv("Data_Lake_SAS_Token")
    container_name=os.getenv("Data_Lake_Container")
    print(account_url,"Account Data Type: ", type(account_url))
    print(sas_url,"SAS Data Type: ", type(sas_url))
    print(container_name,"Container Data Type: ", type(container_name))

with DAG(
    dag_id="new_dag_operator",
    start_date=datetime(2024, 6, 29),
    schedule_interval= None,
    catchup=False,
    default_args={
        "retries": 0

    },
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")
    python_task = PythonOperator(task_id='virtualenv_python',
            python_callable=create_bar_graph
                )
    begin >> python_task >> end