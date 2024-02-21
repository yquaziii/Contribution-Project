from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args dictionary to specify default parameters for the DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(seconds=20)
}

# Instantiate the DAG
dag = DAG(
    'counting_dag_1',
    default_args=default_args,
    description='A DAG to count from 1 to 300 seconds with execution timeout of 20 seconds',
    schedule_interval=None,  # This DAG will be triggered manually or externally
)

# Define the Python function to count and print numbers
def count_and_print():
    for i in range(1, 301):
        print(i)

# Define the PythonOperator task
counting_task = PythonOperator(
    task_id='counting_task',
    python_callable=count_and_print,
    dag=dag,
)

# Set the task dependencies
counting_task

if __name__ == "__main__":
    dag.cli()
