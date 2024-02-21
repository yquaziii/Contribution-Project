from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 12),
    'depends_on_past': False,
    'retries': 1,
}

# Create the DAG
dag = DAG(
    'start_k8s_pod_',
    default_args=default_args,
    description='Start a Kubernetes pod using Airflow',
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,  # You can set this to True if you want to backfill
)

# Define the task that starts the Kubernetes pod
start_k8s_pod = KubernetesPodOperator(
    task_id='start_k8s_pod_task',
    namespace="composer-2-1-11-airflow-2-4-3-e9619468",  # Specify the namespace in which to create the pod
    image="gcr.io/gcp-runtimes/ubuntu_18_0_4",  # Specify the container image for your pod
    name='k8s-pod-name',  # Specify the name of your pod
    in_cluster=True,  # Set to True if running Airflow within the Kubernetes cluster
    get_logs=True,  # Retrieve pod logs
    dag=dag,
)

# Define task dependencies if needed
# Example: start_k8s_pod.set_upstream(another_task)

if __name__ == "__main__":
    dag.cli()
