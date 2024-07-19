from datetime import timedelta
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
import sys
import os
import re
import pandas as pd
import json
from airflow.models.xcom_arg import XComArg
from test import create_bar_graph
with DAG(
    dag_id="demo_pipeline_virtual_operator",
    start_date=datetime(2024, 6, 29),
    schedule_interval= None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "Airflow1",
        "factory_name": "docdigitizer",
        "resource_group_name": "pattern-detection",

    },
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")
    # python_task = ExternalPythonOperator(task_id='virtualenv_python',
    #         python_callable=create_bar_graph,
    #         op_kwargs={'data': [10, 20, 30, 40, 50], 'labels': ['A', 'B', 'C', 'D', 'E'], 'title': 'Sample Bar Graph', 'xlabel':'Categories', 'ylabel':'Values'},
    #         python=str(sys.executable).replace('\\','/')
    #             )
    python_task = create_bar_graph([10, 20, 30, 40, 50],['A', 'B', 'C', 'D', 'E'], 'Sample Bar Graph', 'Categories', 'Values')
    begin >> python_task >> end