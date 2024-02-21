from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import time  # Add this import statement

def sleep_and_print_seconds(task_id, **kwargs):
    start_time = datetime.now()
    print(f"{task_id} started at {start_time}")
    
    # Sleep for 10 seconds
    sleep_duration = 10
    for i in range(sleep_duration + 1):
        print(f"{task_id} - {i} seconds elapsed")
        time.sleep(1)

    end_time = datetime.now()
    print(f"{task_id} completed at {end_time}")
    return "Task completed."

# Define the DAG
dag = DAG(
    'marlboro_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),  # Adjust as needed
)

# Create 100 tasks in the DAG
tasks = []
for i in range(1, 101):
    task_id = f'marlboro-{i}'
    task = PythonOperator(
        task_id=task_id,
        python_callable=sleep_and_print_seconds,
        op_kwargs={'task_id': task_id},
        provide_context=True,
        dag=dag,
    )
    tasks.append(task)

# Set task dependencies to create a linear workflow
for i in range(1, len(tasks)):
    tasks[i] >> tasks[i - 1]
