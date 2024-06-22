import os
import glob
import json
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def get_data_etl(folder):
    log_data = []
    print("Start Cleaning")
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".log"):
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    for line in f:
                        match = re.match(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}),(\w+),(\w+),({.+})', line)
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

    df = pd.DataFrame(log_data, columns=["Time Stamp", "Code", "Event ID", "Message Code", "Event ID", "Event Time", "Ingest Time", 
                                        "Request ID", "Source IP", "HTTP Method", "HTTP Url", "HTTP Auth", "HTTP Auth Hash", 
                                        "Resource", "Resource Class", "Resource Method", "Organization", "App", "User", "Entity", 
                                        "Timestamp Request", "Timestamp Response"])
    columns_to_drop = ['Time Stamp', 'Event ID', 'Ingest Time', 'HTTP Method', 'Message Code', 'Code', 'Request ID', 'HTTP Auth Hash', 'Resource Class', 
                   'Resource Method', 'Entity', 'App', 'User']  
    df_clean = df.drop(columns=columns_to_drop)
    del df
    df = time_format(df_clean)
    print("Completed the data cleaning")
    # plot_graph(df)
    


def time_format(df: pd.DataFrame):
    datetime_columns = ['Event Time', 'Timestamp Request', 'Timestamp Response']
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df['Response_Time'] = df['Timestamp Response'] - df['Timestamp Request']
    df['Response_Time'] = df['Response_Time'].dt.total_seconds()
    return df


def plot_graph(df : pd.DataFrame):
    # Plotting response time distribution
    fig, ax = plt.subplots(figsize=(10, 6))

    # Create a density plot
    sns.kdeplot(df['Response_Time'], fill=True, color='blue', ax=ax)

    # Create a histogram with logarithmic scale on the y-axis
    ax.hist(df['Response_Time'], bins=20, alpha=0.5, density=True, log=True, color='lightblue')

    plt.xlabel('Response Time (Seconds)')
    plt.ylabel('Density')
    plt.title('Response Time Distribution')
    plt.show()
