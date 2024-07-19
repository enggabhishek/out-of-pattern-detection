from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.decorators import task
from airflow.operators.python import  PythonVirtualenvOperator

def create_bar_graph(data, labels, title="Bar Graph", xlabel="X-axis", ylabel="Y-axis"):
    import matplotlib.pyplot as plt
    import os
    ## For gmail account
    """
    Creates and displays a bar graph.
    
    Parameters:
    - data (list): A list of values for the bars.
    - labels (list): A list of labels for each bar.
    - title (str): The title of the graph.
    - xlabel (str): The label for the x-axis.
    - ylabel (str): The label for the y-axis.
    """
    plt.figure(figsize=(10, 6))
    plt.bar(labels, data, color='blue')
    
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    print("Completed the plotting")
    print(os.environ.get("TESTNAME"))
    print("Successfully Tested for gmail account")

with DAG(
    dag_id="new_dag_operator",
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
    python_task = PythonVirtualenvOperator(task_id='virtualenv_python',
            python_callable=create_bar_graph,
            op_kwargs={'data': [10, 20, 30, 40, 50], 'labels': ['A', 'B', 'C', 'D', 'E'], 'title': 'Sample Bar Graph', 'xlabel':'Categories', 'ylabel':'Values'},
            requirements=["matplotlib==3.9.0"],
            system_site_packages=False,
                )
    begin >> python_task >> end