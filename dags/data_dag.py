from datetime import timedelta
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from data_cleaning_etl import get_data_etl, process_complete
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.providers.microsoft.azure.sensors.data_factory import (
    AzureDataFactoryPipelineRunStatusSensor,
)
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
with DAG(
    dag_id="dag_python_pipeline",
    start_date=datetime(2022, 5, 14),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "airflow_pipeline",
        #This is a connection created on Airflow UI
        "factory_name": "docdigitizer",
        "resource_group_name": "pattern-detection",

    },
    default_view="graph",
) as dag:

    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")
    with TaskGroup(group_id="group1") as group1pipeline:
        run_group1_pipeline1 = AzureDataFactoryRunPipelineOperator(
            task_id="get_data_etl", pipeline_name="run_get_data_etl",
            parameters={'folder': 'dags/demo'}
        )
        run_group1_pipeline2 = AzureDataFactoryRunPipelineOperator(
            task_id="task_completed", pipeline_name="run_task_completed"
        )
        run_group1_pipeline1 >> run_group1_pipeline2
    begin >> group1pipeline >> end