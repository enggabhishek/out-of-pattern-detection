from airflow.decorators import task, dag, task_group
import pandas as pd
from datetime import datetime

@dag(start_date=datetime(2024, 6, 29), schedule_interval= None, catchup=False)
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
            print(account_url,"Account Data Type: ", type(account_url))
            print(sas_url,"SAS Data Type: ", type(sas_url))
            print(container_name,"Container Data Type: ", type(container_name))
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
        
    @task(task_id='end')
    def end():
        pass
    
    run = Extracting_and_Transforming_data()
    run2 = Feature_Engineering(run)
    finish = end()
    run2 >> finish
dag_run = extract()