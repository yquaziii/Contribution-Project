from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Function to check cx_Oracle package
def check_cx_oracle():
    try:
        import cx_Oracle
        print("cx_Oracle is present and usable.")
    except ImportError:
        print("cx_Oracle is not installed.")
    except Exception as e:
        print(f"Error: {str(e)}")

# Default arguments for the DAG
default_args = {
    'owner': 'composer-gcp',
    'start_date': datetime(2024, 1, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'check_cx_oracle',
    default_args=default_args,
    description='DAG to check cx_Oracle package in Composer GCP',
    schedule_interval=timedelta(days=1),  # Adjust the schedule as needed
)

# Task to run the check_cx_oracle function
check_cx_oracle_task = PythonOperator(
    task_id='check_cx_oracle',
    python_callable=check_cx_oracle,
    dag=dag,
)

# Define the task order in the DAG
check_cx_oracle_task
