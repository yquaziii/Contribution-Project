from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_count(**kwargs):
    for i in range(1, 301):
        print(i)
        time.sleep(1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(seconds=20),  # Setting execution timeout to 20 seconds
}

dag = DAG(
    'counting_dag',
    default_args=default_args,
    description='A DAG to count from 1 to 300 seconds and print the count.',
    schedule_interval=timedelta(minutes=1),  # You can adjust the schedule interval as needed
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda **kwargs: None,  # Dummy task to start the DAG
    provide_context=True,
    dag=dag,
)

count_task = PythonOperator(
    task_id='count_task',
    python_callable=print_count,
    provide_context=True,
    dag=dag,
)

start_task >> count_task  # Set the task dependencies
