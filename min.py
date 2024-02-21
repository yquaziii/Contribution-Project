from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args
default_args = {
    'owner': 'global',
    'start_date': datetime.now() - timedelta(days=0.50),
    'retries': 3,
    'retry_delay': timedelta(minutes=0.50)
}

# Define the DAG
dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple DAG to print Hello, World!',
    schedule_interval=timedelta(minutes=1),  # Updated to run every 1 minute
)

# Define the task
def print_hello_world():
    print("Hello, World!")

hello_world_task = PythonOperator(
    task_id='print_hello_world',
    python_callable=print_hello_world,
    dag=dag,
)

# Set task dependencies
hello_world_task

if __name__ == "__main__":
    dag.cli()
