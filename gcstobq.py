from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.dummy_operator import DummyOperator


# Define default arguments for the DAG
default_args = {
    'owner': 'yquazi',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG instance
dag = DAG(
    'load_csv_to_bigquery1',
    default_args=default_args,
    schedule_interval=None,  # You can set this to a specific schedule if needed
    catchup=False,
    max_active_runs=1,
)

#Dummy start task
start = DummyOperator(
        task_id='Start',
        dag=dag,
    )

# Define the GCS to BigQuery task
load_csv_to_bigquery = GCSToBigQueryOperator(
    task_id='load_csv_to_bigquery_task',
    bucket='repro-workflow-project',
    source_objects=['industry.csv'],
    destination_project_dataset_table='prod-yq.reproproject.repro-project', 
    schema_fields=[  # Define the schema of your BigQuery table
        {'name': 'column1', 'type': 'STRING', 'mode': 'NULLABLE'},
    #  {'name': 'column2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        # Add more fields as needed
    ],
    write_disposition='WRITE_TRUNCATE',  # Choose WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY
    skip_leading_rows=1,  # Number of rows to skip in the CSV file
    field_delimiter=',',  # CSV field delimiter
    #google_cloud_storage_conn_id='google_cloud_storage_default',  # Connection ID for GCS
    #bigquery_conn_id='bigquery_default',  # Connection ID for BigQuery
    task_concurrency=1,  # Number of parallel tasks
   # autocommit=True,  # Auto commit the transaction
    dag=dag,
)

#Dummy end task
end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# Set task dependencies
load_csv_to_bigquery

if __name__ == "__main__":
    dag.cli()
