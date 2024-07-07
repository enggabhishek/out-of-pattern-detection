import azure.functions as func
import logging
import os
import requests
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):
    token = os.environ.get("Airflow_Token")
    deployment_url = "https://clxyvpjqq0jyj01ncg95bcz16.astronomer.run/dztkarlq"
    dag_id = "virtual_operator"
    
    response = requests.post(
        url=f"{deployment_url}/api/v1/dags/{dag_id}/dagRuns",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        data='{}'
    )
    logging.info(f"Python EventGrid triggered DAG pipelin: {response.json()}")
