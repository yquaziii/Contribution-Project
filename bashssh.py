from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: None,  # Catch and ignore failures
}

dag = DAG(
    'ssh_debug_dag',
    default_args=default_args,
    description='DAG to SSH into VM and store debug output',
    schedule_interval='@daily',  # You can adjust the scheduling as per your requirement
)

# Define the BashOperator task
ssh_task = BashOperator(
    task_id='ssh_task',
    bash_command='ssh -vvv user@10.128.0.8',
    dag=dag,
    retries=1,  # Number of retries before giving up
    retry_delay=timedelta(minutes=5),  # Time between retries
)

# Set task dependencies if needed
# some_other_task >> ssh_task

if __name__ == "__main__":
    dag.cli()
