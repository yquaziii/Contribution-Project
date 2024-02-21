from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define default_args dictionary to set the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Instantiate a DAG
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='@daily',  # Run the DAG daily
)

# Define a Python function to print in the global scope
def top_level_code():
    print("This is top-level code in global scope")

# Use the PythonOperator to execute the top_level_code function
top_level_task = PythonOperator(
    task_id='top_level_task',
    python_callable=top_level_code,
    dag=dag,
)

# Define a Python function to print within the DAG code
def dag_code():
    print("This is DAG code")

# Use the PythonOperator to execute the dag_code function
dag_code_task = PythonOperator(
    task_id='dag_code_task',
    python_callable=dag_code,
    dag=dag,
)

# Set the task dependencies
top_level_task >> dag_code_task
