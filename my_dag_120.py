from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'yquazi-giggle',
    'start_date': datetime(2023, 9, 29),
}

dag = DAG(
    'my_dag_120',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval here
    catchup=False,
)

task1 = BashOperator(
    task_id='task1',
    bash_command='echo "This is the first task"',
    dag=dag,
)

task2 = BashOperator(
    task_id='task2',
    bash_command='echo "This is the second task"',
    dag=dag,
)

task1 >> task2
