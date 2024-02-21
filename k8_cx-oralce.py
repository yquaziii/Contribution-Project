from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
    'owner': 'yquazi',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cx_oracle_example',
    default_args=default_args,
    description='A simple DAG to install cx-Oracle and verify its version',
    schedule_interval=timedelta(days=1),  # Adjust the schedule interval as needed
)

# Task to install cx-Oracle
install_cx_oracle_task = KubernetesPodOperator(
    task_id='install_cx_oracle',
    name='install-cx-oracle',
    image='gcr.io/gcp-runtimes/ubuntu_18_0_4',  # You may need a custom image with cx-Oracle dependencies
    cmds=['pip', 'install', 'cx-Oracle'],
    namespace='composer-2-5-3-airflow-2-6-3-7e4e69f8',
    dag=dag,
)

# Task to verify cx-Oracle version
verify_cx_oracle_version_task = KubernetesPodOperator(
    task_id='verify_cx_oracle_version',
    name='verify-cx-oracle-version',
    image='gcr.io/gcp-runtimes/ubuntu_18_0_4',  # Use the same image for consistency
    cmds=['python', '-c', 'import cx_Oracle; print(cx_Oracle.__version__)'],
    namespace='composer-2-5-3-airflow-2-6-3-7e4e69f8',
    dag=dag,
)

# Set task dependencies
install_cx_oracle_task >> verify_cx_oracle_version_task

if __name__ == "__main__":
    dag.cli()
