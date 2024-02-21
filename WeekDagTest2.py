from datetime import datetime

from airflow import models
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "enterprise",
    'start_date': '2024-2-4',
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": 0,
}

def WeekDagTest5():
    print("WeekDagTest executing....................")

with models.DAG(
    "WeekDagTest5",
    default_args=default_args,
    catchup=False,
    schedule_interval='00 12 * * THU',
    # schedule_interval=None,
) as dag:
    WeekDagTest1 = PythonOperator(
        task_id="WeekDagTest5",
        python_callable=WeekDagTest5,
    )

    WeekDagTest1        
