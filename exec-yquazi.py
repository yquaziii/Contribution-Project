from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_runtime():
    current_time = datetime.now()
    print(f"Task executed at: {current_time}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple DAG to print run time and execution time',
    schedule_interval=timedelta(days=1),  # Adjust the interval as needed
)

print_task = PythonOperator(
    task_id='print_runtime',
    python_callable=print_runtime,
    dag=dag,
)

print_task
