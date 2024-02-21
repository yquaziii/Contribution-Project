from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Import the necessary libraries.
from datetime import date, datetime
# upstream DAG
with DAG(
 dag_id="threeupstream_trigger_dag",
 start_date=datetime(2023, 1, 1),
 schedule="@daily",
) as dag:
 start_task = BashOperator(
 task_id="start_task",
 bash_command="echo 'Start task'",
 )
 trigger_A = TriggerDagRunOperator(
 task_id="trigger_A",
 trigger_dag_id="downstream_dag_A",
 conf={"message":"Message to pass to downstream DAG A."},
 )
 trigger_B = TriggerDagRunOperator(
 task_id="trigger_B",
 trigger_dag_id="downstream_dag_B",
 )
 start_task >> trigger_A >> trigger_B