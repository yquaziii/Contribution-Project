from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default_args dictionary
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 10, 8),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'example_dag_300tasks',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule_interval to None for manual triggering
    catchup=True  # Enable backfill parameter
)

# Function to print task name and sleep
def print_and_sleep(task_name, **kwargs):
    print(f"Running task: {task_namee}")
    import time
    time.sleep(1800)  # Sleep for 30 minutes

# Create 10 tasks in the DAG
for i in range(1, 300):
    task_name = f'task_{i}'
    task = PythonOperator(
        task_id=task_name,
        python_callable=print_and_sleep,
        op_args=[task_name],
        dag=dag,
    )

    if i > 1:
        # Set task dependencies (except for the first task)
        task.set_upstream(dag.get_task(f'task_{i - 1}'))

# Define the task execution order
for i in range(1, 300):
    task_list[i] >> task_list[i + 1]

# Define the task execution order
#task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6 >> task_7 >> task_8 >> task_9 >> task_10
