from airflow.decorators import task, dag, task_group
import pandas as pd
from datetime import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import json
from elasticsearch import Elasticsearch
import os
import numpy as np
# from dotenv import load_dotenv
# load_dotenv()
account_url=os.getenv("DATA_LAKE_URL")
sas_url=os.getenv("DATA_LAKE_SAS_TOKEN")
container_name=os.getenv("DATA_LAKE_CONTAINER")
elasticsearch_CloudID=os.getenv("ELASTIC_CLOUD_ID")
elastic_pwd=os.getenv("ELASTIC_PASSWORD")
es = Elasticsearch(
                cloud_id = elasticsearch_CloudID,
                basic_auth=("elastic", elastic_pwd)
                )
#==============Producer Callable Function==================
def prod_function(data):
    df = pd.read_json(data)
    for _, row in df.iterrows():
        yield json.dumps(_), json.dumps(row.to_dict()).encode('utf-8')
        
#==============Consumer Callable Function==================       
def consume_function(message):
    key = json.loads(message.key())
    message_content = json.loads(message.value().decode('utf-8'))
    body ={
                        "Event Time": pd.to_datetime(message_content['Event Time'], format="%b %d, %Y @ %H:%M:%S.%f").isoformat(),
                        "TimeStamp Request": pd.to_datetime(message_content['Timestamp Request'], format="%b %d, %Y @ %H:%M:%S.%f").isoformat(),
                        "TimeStamp Response": pd.to_datetime(message_content['Timestamp Response'], format="%b %d, %Y @ %H:%M:%S.%f").isoformat(),
                        "HTTP Method": message_content['HTTP Method'],
                        "HTTP URL": message_content['HTTP Url'],
                        "Transformed_HTTP_Auth": message_content['Transformed_HTTP_Auth'],
                        "Resource": message_content['Resource'],
                        "Organization": message_content['Organization'],
                        "Heavy Load": message_content['HeavyLoad'],
                        "Source IP": message_content['SourceIP']
                        } 
    es.index(index='http_logs', body=body)
    
    print(f"Key >> {key}")

#============================Defining DAG===============================================
@dag(start_date=datetime(2023, 1, 29), schedule_interval= None, catchup=False)
def extract():
    
    @task(task_id='begin')
    def begin():
        pass
    start = begin()
    
    @task_group()
    def Extracting_and_Transforming_data(account_url,sas_url, container_name):
        #======================================Data Cleaning Method==========================================    
        @task.virtualenv(task_id='complete_data_cleaning_etl',requirements=["azure-storage-blob==12.20.0"],system_site_packages=True)
        def get_data_etl(account_url,sas_url, container_name):
            import json
            import re
            import pandas as pd
            from azure.storage.blob import BlobServiceClient
            import io
            
            #===========================Connect with Azure Data Blob Storage===========================
            blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_url)
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            log_data = []
            for file in blob_list:
                if file.name.endswith(".log"):
                    content = container_client.get_blob_client(file.name).download_blob().readall()
                    decoded_content = content.decode('utf-8')
                    decoded_content = io.StringIO(decoded_content)
                    for line in decoded_content:
                        match = re.match(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}),(\w+),(\w+),({.+})', line)
                        print(match)
                        if match:
                            timestamp, code, event_id, payload = match.groups()
                            try:
                                payload_json = json.loads(payload)
                                nested_json = json.loads(payload_json["b64Payload"])
                                log_data.append([
                                                timestamp, code, event_id,
                                                payload_json["MessageCode"],
                                                payload_json["EventId"],
                                                payload_json["EventTime"],
                                                payload_json["IngestTime"],
                                                nested_json["requestId"],
                                                nested_json["sourceIp"],
                                                nested_json["httpMethod"],
                                                nested_json["httpUrl"],
                                                nested_json["httpAuth"],
                                                nested_json["httpAuthHash"],
                                                nested_json["resource"],
                                                nested_json["resourceClass"],
                                                nested_json["resourceMethod"],
                                                nested_json["organization"],
                                                nested_json["app"],
                                                nested_json["user"],
                                                nested_json["entity"],
                                                nested_json["timestamp_req"],
                                                nested_json["timestamp_resp"]
                                            ])
                            except (ValueError, KeyError):
                                print(f"Failed to parse payload: {payload}")
                        else:
                            print("No match found")
            df = pd.DataFrame(log_data, columns=["TimeStamp", "Code", "EventID1", "MessageCode", "EventID2", "Event Time", "Ingest Time", 
                                                    "RequestID", "SourceIP", "HTTP Method", "HTTP Url", "HTTP Auth", "HTTPAuthHash", 
                                                    "Resource", "ResourceClass", "ResourceMethod", "Organization", "App", "User", "Entity", 
                                                    "Timestamp Request", "Timestamp Response"])
            columns_to_drop = ['TimeStamp', 'EventID1',"EventID2", 'MessageCode', 'Code', 'RequestID', 'HTTPAuthHash', 'ResourceClass', 
                        'ResourceMethod', 'Entity', 'App', 'User']  
            df_clean = df.drop(columns=columns_to_drop)
            del df, log_data
            return df_clean

        #==========================================Changing Time Format===============================================
        @task(task_id='time_format')
        def time_format(df):
            dtypes = {'Event Time': 'datetime64[ns]',
                'Timestamp Request': 'datetime64[ns]',
                'Timestamp Response': 'datetime64[ns]'}
            # Convert the columns to their respective datatypes:
            df = df.astype(dtypes)
            datetime_columns = ['Timestamp Request', 'Timestamp Response']
            for col in datetime_columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            df['Response Time'] = df['Timestamp Response'] - df['Timestamp Request']
            df['Response Time'] = df['Response Time'].dt.total_seconds()
            datetime_columns = ['Timestamp Request', 'Timestamp Response','Ingest Time']
            for col in datetime_columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            df['Timestamp Request'] = df['Timestamp Request'].dt.strftime("%b %d, %Y @ %H:%M:%S.%f")
            df['Timestamp Response'] = df['Timestamp Response'].dt.strftime("%b %d, %Y @ %H:%M:%S.%f")
            df['Event Time'] = df['Event Time'].dt.strftime("%b %d, %Y @ %H:%M:%S.%f")
            return df
        #========================================Creating Response Time Variable=============================================
        extract = get_data_etl(account_url,sas_url, container_name)
        start >> extract
        transform = time_format(extract)
        return transform
#==========================================Feature Engineering Task Group=========================================
    @task_group()
    def Feature_Engineering(df):
        @task(task_id='target_variable')
        def target_variable(df):
            # Calculate the first quartile (Q1) and third quartile (Q3) of 'time_difference'
            Q1 = df['Response Time'].quantile(0.25)
            Q3 = df['Response Time'].quantile(0.75)
            # Calculate the interquartile range (IQR)
            IQR = Q3 - Q1
            # Define the lower and upper bounds for outliers
            lb = Q1 - 1.5 * IQR
            ub = Q3 + 1.5 * IQR
            # Create a new column 'if_highload'
            df['HeavyLoad'] = df['Response Time'].apply(lambda x: 1 if x > ub else 0)
            df['Transformed_HTTP_Auth'] = np.where(df['HTTP Auth'].str.contains('USER_KEY'), 'User', 'Organization')
            print(df['HeavyLoad'].head())
            print("Completed the data cleaning")
            return df
        load = target_variable(df)
        return load

        #===========================================================================
#==========================================Kafka Streaming To ElasticSearch Task Group=========================================
                       
    @task_group()
    def KafkaStreamingToElasticSearch(df):
        @task(task_id = 'get_dataframe')
        def get_dataframe(df):
            return df.to_json()
        
        #=====================Producer Task===================================================       
        producer = ProduceToTopicOperator(
        task_id="Producer",
        kafka_config_id="Kafka_connect",
        topic="httplogs",
        producer_function=prod_function,
        producer_function_args=["{{ ti.xcom_pull(task_ids='KafkaStreamingToElasticSearch.get_dataframe')}}"],
        poll_timeout=10)
        
        #================================Consumer Task=============================================
        
        consumer = ConsumeFromTopicOperator(
        task_id="Consumer",
        kafka_config_id="Kafka_connect",
        topics= ["httplogs"],
        apply_function=consume_function,
        poll_timeout=90
        )
        #===========================Calling Producer and Consumer Tasks============================
        [get_dataframe(df)] >> producer
        producer >> consumer
        
        
    @task(task_id='end')
    def end():
        pass
    
    run = Extracting_and_Transforming_data(account_url,sas_url, container_name)
    run2 = Feature_Engineering(run)
    run3 = KafkaStreamingToElasticSearch(run2)
    finish = end()
    run3 >> finish
dag_run = extract()