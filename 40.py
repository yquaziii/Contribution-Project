from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args dictionary to specify the default parameters for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG with 40 tasks',
    schedule_interval=timedelta(days=1),
)

# Define a Python function to be executed by each task
def print_task_number(task_number):
    print(f"Executing Task {task_number}")

# Create 40 tasks in a loop
tasks = []
for i in range(1, 41):
    task = PythonOperator(
        task_id=f'task_{i}',
        python_callable=print_task_number,
        op_args=[i],
        dag=dag,
    )
    tasks.append(task)

# Set task dependencies (task i depends on task i-1)
for i in range(1, 40):
    tasks[i] >> tasks[i-1]

# You can add more complex dependencies based on your requirements

if __name__ == "__main__":
    dag.cli()