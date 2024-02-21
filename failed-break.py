import datetime
from airflow import models
from airflow.operators import bash
from airflow.operators.python_operator import PythonOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    "owner": "Composer Example",
    "depends_on_past": True,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY,
}

def printGreeting(*op_args):
    raise Exception("Not going to print this!")

with models.DAG(
    "simple_dag_with_two_tasks_not_working-dop",
    catchup=False,
    default_args=default_args,
    schedule_interval='*/20 * * * *',
) as dag:
    # Print the dag_run id from the Airflow logs
    task_1 = PythonOperator(
        task_id="print_hello",
        python_callable=printGreeting,
        op_args=["hello"]
    )

    task_2 = PythonOperator(
        task_id="print_goodbye",
        python_callable=printGreeting,
        op_args=["goodbye"]
    )

    task_1 >> task_2
