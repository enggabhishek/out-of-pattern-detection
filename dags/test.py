from airflow.decorators import task, dag
from datetime import timedelta
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from elasticsearch import Elasticsearch
import os
elasticsearch_CloudID=os.getenv('ELASTIC_CLOUD_ID')
elastic_pwd=os.getenv('ELASTIC_PASSWORD')
es = Elasticsearch(
                cloud_id = elasticsearch_CloudID,
                basic_auth=("elastic", elastic_pwd)
                )
@dag(start_date=datetime(2024, 6, 29), schedule_interval= None, catchup=False)
def checking_ti_xcom():
    
    @task.virtualenv(use_dill=True,system_site_packages=True)
    def create_bar_graph():
        data = ["Abhishek", "Harsh","Kishori"]
        return data
        
    @task(task_id='get_the_name')
    def print_the_name(data: list):
        for i in data:
            print(i)
    data = create_bar_graph()
    print_the_name(data)

run = checking_ti_xcom()