from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

# Define your Kubernetes configuration here
kube_config = {
    'image': "gcr.io/gcp-runtimes/ubuntu_18_0_4",
    'cmds': "kubectl top pod -n=kube-system",
    'requests': {'cpu': '1m', 'memory': '10Mi'},
    'namespace': "default",
    'name': 'my-pod',
    'in_cluster': True  # Set to True if running inside the cluster, False if outside
}

# Define your DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 5),
    'retries': 1,
}

dag = DAG(
    'k8s_pod_operator_example',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,  # Set to False if you don't want to backfill
)

# Create a KubernetesPodOperator task
create_pod = KubernetesPodOperator(
    task_id='create_pod_task',
    dag=dag,
    **kube_config  # Pass in the Kubernetes configuration
)

# Define task dependencies here if needed

if __name__ == "__main__":
    dag.cli()
