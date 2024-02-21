from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="airflow_db_dag",
    start_date=datetime(2024, 1, 31),
    schedule_interval=None,
          catchup=False,
      ) as dag:
          create_pet_table = SQLExecuteQueryOperator(
              conn_id="airflow_db",
              task_id="airflow_db_sql",
              sql="""
                SELECT * FROM dag LIMIT 10;;
              """,
              show_return_value_in_logs = True
      )
airflow_db_sql