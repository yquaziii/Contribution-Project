from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'timeout_dag',
    default_args=default_args,
    description='DAG with execution timeout and time counter',
    schedule_interval=None,  # You can set the schedule_interval as needed
    catchup=False,
)

tasks = []  # List to store tasks

def print_elapsed_time(**kwargs):
    execution_time = kwargs.get('execution_time', datetime.utcnow())
    elapsed_time_seconds = (execution_time - kwargs['ti'].start_date).total_seconds()
    print(f"Elapsed time: {elapsed_time_seconds} seconds")

# Create tasks with a timeout of 20 seconds
for i in range(300 // 20):
    task = PythonOperator(
        task_id=f'task_{i+1}',
        python_callable=print_elapsed_time,
        provide_context=True,
        execution_timeout=timedelta(seconds=20),
        dag=dag,
    )
    tasks.append(task)  # Append task to the list

# Set task dependencies using the list
for i in range(1, len(tasks)):
    tasks[i].set_upstream(tasks[i-1])

# Set the DAG to end
dag_end = PythonOperator(
    task_id='dag_end',
    python_callable=lambda **kwargs: print("DAG execution completed."),
    provide_context=True,
    dag=dag,
)

# Set the final task dependency
dag_end.set_upstream(tasks[-1])  # Use the last task in the list
