from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GoogleCloudStorageCreateBucketOperator, GoogleCloudStorageUploadOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import random

#DAG Initialization: Define the default arguments and create a DAG instance.

default_args = {
    'owner': 'yquazi-giggles',
    'start_date': datetime(2023, 10, 25),
    'retries': 1,
}

dag = DAG('generate_and_store_to_gcs', default_args=default_args, schedule_interval=None)

#Generate Random Files: Create a Python function that generates random files. You can customize the number and size of files as per your requirements.

def generate_random_files():
    output_dir = '/path/to/worker/directory'
    os.makedirs(output_dir, exist_ok=True)
    for i in range(10):
        file_name = f'random_file_{i}.txt'
        with open(os.path.join(output_dir, file_name), 'w') as file:
            file.write('Random file content')

#Upload Files to GCS: Create a Python function that uploads the generated files to a GCS bucket.
def upload_files_to_gcs():
    gcs_bucket_name = 'testdp11'
    gcs_object_prefix = 'random_files/'
    source_directory = '/path/to/worker/directory/*'
    
    upload_task = GoogleCloudStorageUploadOperator(
        task_id='upload_to_gcs',
        src=source_directory,
        dst=gcs_object_prefix,
        bucket_name=gcs_bucket_name,
        google_cloud_storage_conn_id='google_cloud_default',
        dag=dag
    )

#Define Task Dependencies: Define the dependencies between the tasks.

generate_files_task = PythonOperator(
    task_id='generate_random_files',
    python_callable=generate_random_files,
    dag=dag
)

generate_files_task >> upload_task

# Only if bucket creation is needed
##create_bucket_task >> upload_task  