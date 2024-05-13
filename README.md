# In this project we have used python language.
### We have used 'Jupyter Notebook' and 'JupyterLab' to execute all the codes mentioned in this file.
### Download the Zip file 'logs_202212.zip' (which contains logs of http requests sent to DocDigitizer site in December 2022) from the following link:
https://drive.google.com/drive/folders/114RrgWAgRwqSQTBvq4Ee_-UqYmnAeSat?usp=sharing
### Before proceeding further you need to create separate environments for JupyterNotebook and JupyterLab to avoid overriding of various functions in different libraries

### Library required in JupyterLab:
* pip install numpy
* pip install pandas
* pip install panel
* pip install holoviews
* pip install hvplot
* pip install cartopy
* pip install scipy

### Libraries required in Jupyter Notebook:
* pip install pandas
* pip install matplotlib
* pip install numpy
* pip install scikit-learn
* pip install geoip2
* pip install ipaddress
* pip install seaborn

### Download GEOIP address database to validate Source Ips
'dbip-location-2023-05.mmdb'


### Data Cleaning Process in 'Jupyter Notebook':
* Copy all the .log files in the single folder
* Change the directory in the following code mentioned in the 'Data_Cleaning.ipynb' file as per the location of .log files:
   - folder = 'C:/Users/XYZ/LogFiles/'
* You also need to change the directory location to save the .csv file mentioned in the code:
  - df.to_csv('C:/Users/XYZ/df.csv', index=False)
* Now run the code mentioned in 'Data_Cleaning.ipynb' file.

### Exploratory Data Analysis in 'Jupyter Notebook':
* Read the .csv file which you have created after data cleaning process by changing the directory in the following code written in 'EDA_of_Combined_File.ipynb':
  - df = pd.read_csv('C:/Users/XYZ/df.csv')
* Make sure that you have downloaded the latest version of GEOIP database and modified the code below as per the the file location:
  - reader = geoip2.database.Reader('C:/Users/Users/XYZ/dbip-location-2023-05.mmdb')
* We have created a piece of code to analyze the highest per minute request in each hour for everyday in the month of December.
* We have tried to store the resultset in the another .csv file mentioned in the following code:
  - t_df.to_csv('hourly_basis_Count.csv')
* Since we have 17 million records in total so we have divided the large dataset into small dataset using startified sampling and tried to store all the variables into .csv mentioned in the following code:
  - X_test.to_csv('final_train_test_data.csv') '''You can modify the dataset size as per your requirement or you can use it as whole data'''
* Now run the code mentioned in 'EDA_of_Combined_File.ipynb' file.

### Predictive Analysis in 'JupyterLab':
* Read the .csv file which you have created after Exploratory Data Analysis by changing the directory in the following code written in 'Predictive_Analysis.ipynb':
  - df = pd.read_csv('final_train_test_data.csv')
* Now run the code mentioned in 'Predictive_Analysis.ipynb' file.

### Execute Dashboard-1 using JupyterLab:
* Run JupyterLab application
*  Find the directory to access 'Dashboard.ipynb' file
*  Read the .csv file which you have created after Exploratory Data Analysis by changing the directory in the following code written in 'Dashboard.ipynb':
  - df = pd.read_csv('final_train_test_data.csv') '''You can also use the large dataset as per your requirement and modify the code in the data cleaning process'''
* Now run the code mentioned in 'Dashboard.ipynb' file.
* Finally open the Terminal showing in the JupyterLab application:
  - Run the code 'panel serve Dashboard.ipynb' (Make sure the Terminal you have opened is in the same folder where 'Dashboard.ipynb' file exists)
  - Once the code will be executed successfully you will be able to see the URL similar to this: http://localhost<>/Dashboard (Copy and paste in the address bar of the browser)
* In order to close the running Dashboard process you need to:
  - Find a PID number which has been generated when you run the code in step 4 and copy that number.
  - Open a new Terminal and it should also be in the same folder where 'Dashboard.ipynb' file exists and then execute the following code by replacing '<PID>' keyword with valid PID number: 'taskkill /PID <PID> /F'
![image](https://github.com/enggabhishek/Out-of-Pattern-Detection/assets/29338852/c1502938-9191-4b71-bfb7-771e0aee2063)

    
### Execute Dashboard-2 using JupyterLab
* Repeat all the steps mentioned for Dashboard-1 process in the above and replace the file name from 'Dashboard.ipynb' to 'Dashboard2.ipynb'
