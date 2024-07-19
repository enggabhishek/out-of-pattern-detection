import matplotlib.pyplot as plt
from airflow.decorators import task

@task(task_id='virtualenv_python')
def create_bar_graph(data, labels, title, xlabel, ylabel):
   
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
    plt.show()