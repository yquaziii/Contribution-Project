from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time

def print_seconds():
    for i in range(1, 301):
        print(f"Current second: {i}")
        time.sleep(1)

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'count_seconds_dag_1',
    default_args=default_args,
    description='A simple DAG to count seconds and print them',
    schedule_interval=timedelta(seconds=1),  # Run every second
    catchup=False,
)

count_seconds_task = PythonOperator(
    task_id='count_seconds_task',
    python_callable=lambda: print_seconds(),
    dag=dag,
    execution_timeout=timedelta(seconds=20),  # Set execution timeout to 20 seconds
)

count_seconds_task
