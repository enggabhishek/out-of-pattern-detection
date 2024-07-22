# Project Description
Developed and deployed predictive models to identify and flag anomalous patterns in 16 GB of HTTP request log data, significantly enhancing Docdigitizer's cybersecurity threat detection capabilities.

### Task 1
Build `Blob Storage` based `Azure Data Lake Storage Gen2`.
![alt text](BlobStorageContainer.png)

### Task 2:
Build `Managed Identities` in MS Azure.

### Task 3:
Set up `Apache Kafka` Client.

### Task 4:
Set up `ElasticSearch Cloud` for OLAP.

### Task 5:
Create Apache Airflow DAG on Astronomer platform.
  - Set up Apache Kafka Connection in 'Admin' Tab
  - Extract log files from MS Azure Blob Storage.
  - Transform data using Data Cleaning and Feature Engineering.
  - Streaming log data to ElasticSearch Cloud by ingesting files through Apache Kafka.

### Task 6:
Set up Kibana for building Dashboard on Elastic cloud platform.

### Task 7:
Create `MS Azure Function App`.
![alt text](Azure_Function_App.png)

### Task 8:
Create Pipeline in `MS Azure Data Factory` for triggering Apache Airflow DAG using `Storage Event`.
![alt text](StorageEventTriggerDataFactory.png)


## Steps to Perform Exploratory Data Analysis on Live Stream Data:
  - Set up the GitHub Repository.
  - Set up Git Action for CI/CD process for deploying DAG file directly into Astrnomer Airflow Cloud.
  - Once the log file will be created/uploaded in the Azure Blob Storage then it will initiate Storage Event Trigger.
  - Then Apache `Airflow` DAG on Astronomer cloud will be triggered:
  ![alt text](Airflow_DAG.png)
  - After the data is ingested into `Elastic` Cloud, it can be utilized to create a Kibana dashboard, which will display the HTTP request patterns over various time periods:
![alt text](Kibana_Dashboard.png)

### Steps to Perform Predictive Analytics:
  - After completion of EDA, export the Data from `Kibana` into csv which includes additional attributes created in Kibaba using `Lucene Expressions`
  - Achieved 93% accuracy in load factor estimation and anomaly detection using Logistic Regression, Decision Tree, and Random Forest models, enhancing system reliability.
