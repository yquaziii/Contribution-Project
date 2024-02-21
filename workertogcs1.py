from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSFileUploadOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from google.cloud import storage

default_args = {
    'owner': 'your_name',
    'start_date': days_ago(1),
}

dag = DAG(
    'generate_and_store_to_gcs',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def generate_random_files(**kwargs):
    # Generate random files
    output_dir = '/path/to/worker/directory'
    os.makedirs(output_dir, exist_ok=True)
    for i in range(10):
        file_name = f'random_file_{i}.txt'
        with open(os.path.join(output_dir, file_name), 'w') as file:
            file.write('Random file content')

generate_files_task = PythonOperator(
    task_id='generate_random_files',
    python_callable=generate_random_files,
    provide_context=True,
    dag=dag,
)

gcs_bucket_name = 'testdp11'
gcs_object_prefix = 'random_files/'

# Create a bucket
#def create_gcs_bucket():
 #   client = storage.Client()
  #  bucket = client.bucket(gcs_bucket_name)
   # if not bucket.exists():
    #    bucket.create(location='us')

#create_bucket_task = PythonOperator(
    #task_id='create_gcs_bucket',
    #python_callable=create_gcs_bucket,
    #dag=dag,
#)

# Upload files to GCS
upload_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src='/path/to/worker/directory/',
    dst=gcs_object_prefix,
    bucket_name=gcs_bucket_name,
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag,
)

generate_files_task >>  upload_task
