from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import random
import string
from google.cloud import storage

# Define your GCS bucket and owner
bucket_name = 'testdp11'
owner = 'yquazi-giggles'

# Function to generate random files
def generate_random_file(file_path):
    random_data = ''.join(random.choice(string.ascii_letters) for _ in range(1024))  # Generate 1KB of random data
    with open(file_path, 'w') as file:
        file.write(random_data)

# Function to upload files to GCS
def upload_to_gcs(bucket_name, source_path, destination_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_filename(source_path)

# Define default arguments for the DAG
default_args = {
    'owner': owner,
    'start_date': days_ago(1),
    'retries': 1,
}

# Create the DAG
dag = DAG(
    'random_files_to_gcs',
    default_args=default_args,
    schedule_interval=None,  # You can set this to your desired schedule
    catchup=False,
)

# Directory where random files will be generated
output_directory = '/tmp/random_files/'

# Task to generate random files
generate_files_task = PythonOperator(
    task_id='generate_random_files',
    python_callable=generate_random_file,
    op_args=[os.path.join(output_directory, 'random_file1.txt')],
    dag=dag,
)

# Task to upload files to GCS
upload_to_gcs_task = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_args=[bucket_name, os.path.join(output_directory, 'random_file1.txt'), 'random_files/random_file1.txt'],
    dag=dag,
)

# Set the task dependencies
generate_files_task >> upload_to_gcs_task

if __name__ == "__main__":
    dag.cli()
