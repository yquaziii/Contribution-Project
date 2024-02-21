import airflow
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


with airflow.DAG(
    'k8s',
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval='@once') as dag:
  KubernetesPodOperator(
            name='test-k8s-operator',
            # The default timeout is 120 seconds, however pod creation and
            # grabbing the image takes up nearly all of that time. Thus we set
            # it to 10 minutes to be safe.
            startup_timeout_seconds=600,
            namespace='default',
            task_id='run-gsutil-k8s',
            image='google/cloud-sdk:latest',
            cmds=['gsutil'],
            arguments=[
                #'ls',
                #'gs://' + 'us-central1-csr-2-latest-244c0590-bucket',
            ])