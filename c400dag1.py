from google.cloud import storage
from datetime import datetime, timedelta

template = """
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

def create_dag(dag_id, schedule_interval, start_date):
    default_args = {{
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': start_date,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }}

    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'DAG to perform heavy calculation and print PyPI packages - {dag_id}',
        schedule_interval=schedule_interval,
    )

    def perform_heavy_calculation():
        # Your heavy calculation code here
        result = 2 ** 1000000
        return result

    heavy_calculation_task = PythonOperator(
        task_id=f'{dag_id}_perform_heavy_calculation',
        python_callable=perform_heavy_calculation,
        dag=dag,
    )

    def print_all_pypi_packages():
        subprocess.run(['pip', 'list'])

    print_pypi_packages_task = PythonOperator(
        task_id=f'{dag_id}_print_all_pypi_packages',
        python_callable=print_all_pypi_packages,
        dag=dag,
    )

    heavy_calculation_task >> print_pypi_packages_task

    return dag

dag_id = "{dag_id}"
schedule_interval = timedelta(days=1)  # Run the DAG daily
start_date = datetime(2024, 1, {start_day})

dag_instance = create_dag(dag_id, schedule_interval, start_date)
"""

# Google Cloud Storage bucket details
#bucket_name = "gs://byob-composer-11/" 
bucket_name = "byob-composer-11"
project_id = "prod-yq"

# Create a GCS client
storage_client = storage.Client(project=project_id)

# Specify the GCS bucket
bucket = storage_client.bucket(bucket_name)

# Create DAG files in GCS bucket
for i in range(1, 401):
    dag_id = f'heavy_calculation_and_pypi_{i}'
    start_day = i
    file_content = template.format(dag_id=dag_id, start_day=start_day)

    file_name = f'{dag_id}.py'
    blob = bucket.blob(file_name)

    blob.upload_from_string(file_content)

    print(f"DAG file '{file_name}' uploaded to GCS bucket.")

print("DAG files uploaded to GCS bucket successfully.")
