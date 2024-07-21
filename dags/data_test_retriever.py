from airflow.decorators import task, dag, task_group
import pandas as pd
from datetime import datetime


@dag(start_date=datetime(2023, 1, 29), schedule_interval= None, catchup=False)
def extract():
    
    @task(task_id='begin')
    def begin():
        pass
    start = begin()
    
    @task_group()
    def Extracting_and_Transforming_data():
        #======================================Data Cleaning Method==========================================    
        @task.virtualenv(task_id='complete_data_cleaning_etl',requirements=["azure-storage-blob==12.20.0","python-dotenv==1.0.1"],system_site_packages=True)
        def get_data_etl():
            import json
            import re
            import pandas as pd
            from azure.storage.blob import BlobServiceClient
            import io
            import os
            from dotenv import load_dotenv
            load_dotenv()
            # This is the final testing
            account_url=os.getenv("DATA_LAKE_URL")
            sas_url=os.getenv("DATA_LAKE_SAS_TOKEN")
            container_name=os.getenv("DATA_LAKE_CONTAINER")
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
        extract = get_data_etl()
        start >> extract
        transform = time_format(extract)
        return transform
    @task_group()
    def Feature_Engineering(data):
        @task(task_id='target_variable')
        def target_variable (df):
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
            print(df['HeavyLoad'].head())
            print("Completed the data cleaning")
        load = target_variable(data)
        return load
        
    @task_group()
    def KafkaStreamingToElasticSearch(df):
        @task.virtualenv(task_id='KafkaProducer',requirements=["kafka-python==2.0.2","python-dotenv==1.0.1"],system_site_packages=True)
        def KafkaProducer(df):
            import os
            import json
            from kafka import KafkaProducer
            from dotenv import load_dotenv
            load_dotenv()
            sasl_plain_username=os.getenv("UPSTASH_KAFKA_REST_USERNAME")
            sasl_plain_password=os.getenv("UPSTASH_KAFKA_REST_PASSWORD")
            
            producer = KafkaProducer(
                        bootstrap_servers='massive-goldfish-12710-us1-kafka.upstash.io:9092',
                        sasl_mechanism='SCRAM-SHA-256',
                        security_protocol='SASL_SSL',
                        sasl_plain_username=sasl_plain_username,
                        request_timeout_ms=100000, 
                        sasl_plain_password=sasl_plain_password,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8')
                    )
            count_producer=0
            for _, row in df.iterrows():
                count_producer+=1
                producer.send('httplogs', row.to_dict())
            return count_producer
         
        @task.virtualenv(task_id='SendingLogsToElasticSearch', requirements=["kafka-python==2.0.2","elasticsearch==8.14.0","python-dotenv==1.0.1"],system_site_packages=True)   
        def KafkaConsumer(count_producer):
            import os
            import json
            from kafka import KafkaConsumer
            from dotenv import load_dotenv
            load_dotenv()
            from elasticsearch import Elasticsearch
            
            sasl_plain_username=os.getenv("UPSTASH_KAFKA_REST_USERNAME")
            sasl_plain_password=os.getenv("UPSTASH_KAFKA_REST_PASSWORD")
            elasticsearch_CloudID=os.getenv('ELASTIC_CLOUD_ID')
            elastic_pwd=os.getenv('ELASTIC_PASSWORD')
            
            consumer = KafkaConsumer(
                        'httplogs',
                        bootstrap_servers='massive-goldfish-12710-us1-kafka.upstash.io:9092',
                        sasl_mechanism='SCRAM-SHA-256',
                        security_protocol='SASL_SSL',
                        sasl_plain_username=sasl_plain_username,
                        sasl_plain_password=sasl_plain_password,
                        group_id='Abhishek',
                        auto_offset_reset='earliest',
                        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                    )
            es = Elasticsearch(
                cloud_id = elasticsearch_CloudID,
                basic_auth=("elastic", elastic_pwd)
                )
            
            count_consumer = 0
            for message in consumer:
                record = message.value
                count_consumer+=1
                if count_consumer == count_producer:
                    print('Completed the process')
                    break
                body ={
                        "Event Time": pd.to_datetime(record['Event Time'], format="%b %d, %Y @ %H:%M:%S.%f").isoformat(),
                        "TimeStamp Request": pd.to_datetime(record['Timestamp Request'], format="%b %d, %Y @ %H:%M:%S.%f").isoformat(),
                        "TimeStamp Response": pd.to_datetime(record['Timestamp Response'], format="%b %d, %Y @ %H:%M:%S.%f").isoformat(),
                        "HTTP Method": record['HTTP Method'],
                        "HTTP URL": record['HTTP Url'],
                        "HTTP Auth": record['HTTP Auth'],
                        "Resource": record['Resource'],
                        "Organization": record['Organization'],
                        "Heavy Load": record['HeavyLoad']
                        } 
            es.index(index='http_logs', body=body)
        prod = KafkaProducer(df)
        cons = KafkaConsumer(prod)
        
        
    @task(task_id='end')
    def end():
        pass
    
    run = Extracting_and_Transforming_data()
    run2 = Feature_Engineering(run)
    run3 = KafkaStreamingToElasticSearch(run2)
    finish = end()
    run3 >> finish
dag_run = extract()