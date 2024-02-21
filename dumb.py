from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import pandas as pd
import io

# Set your GCS bucket and file information
gcs_bucket_name = 'us-central1-dew-e9619468-bucket/sinklog'
gcs_file_name = 'dummy_data.csv'

# Define a function to generate and upload dummy data to GCS
def generate_and_upload_dummy_data(*args, **kwargs):
    # Generate dummy data
    data = {'Column1': [1, 2, 3, 4, 5],
            'Column2': ['A', 'B', 'C', 'D', 'E']}
    df = pd.DataFrame(data)

    # Convert DataFrame to CSV format
    csv_data = df.to_csv(index=False)

    # Upload data to GCS
    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_file_name)
    blob.upload_from_string(csv_data)

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'send_file_to_gcs',
    default_args=default_args,
    description='A simple DAG to send a file to GCS every minute',
    schedule_interval=timedelta(minutes=1),
)

# Define the task to generate and upload dummy data
generate_and_upload_task = PythonOperator(
    task_id='generate_and_upload_task',
    python_callable=generate_and_upload_dummy_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
generate_and_upload_task

if __name__ == "__main__":
    dag.cli()
