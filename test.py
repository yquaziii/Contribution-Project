from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from kubernetes.client import models as k8s


from airflow.utils.trigger_rule import TriggerRule
from airflow.datasets import Dataset


with DAG(
    dag_id="demo_v2",
    catchup=False,
    start_date=days_ago(1),
    default_args={"retries": 0},
    params={
        "dbt_exit_code": Param(
            default=0,
            type="integer",
        )
    },
) as dag:
    task_dbt = KubernetesPodOperator(
        dag=dag,
        task_id="dbt",
        name="dbt",
        cmds=[
            "/bin/bash",
            "-c",
        ],
        arguments=[
            "set +e;"
            'bash -c "exit {{ params.dbt_exit_code }}";'
            "echo $? > /airflow/xcom/return.json"
        ],
        namespace="composer-user-workloads",
        image="google/cloud-sdk:437.0.0-alpine",
        image_pull_policy="Always",
        do_xcom_push=True,
        config_file="/home/airflow/composer_kube_config",
        # Identifier of connection that should be used
        kubernetes_conn_id="kubernetes_default",
    )

    @task.branch(task_id="identification")
    def identify_failure_type(dbt_exit_code):
        # print(repr(dbt_exit_code))
        if dbt_exit_code == 1:
            return "dbt_failure"
        elif dbt_exit_code == 2:
            return "infra_failure"
        elif dbt_exit_code == 0:
            return "success"

    @task
    def dbt_failure():
        raise AirflowException("DBT task failed")

    @task
    def infra_failure():
        raise AirflowException("Infra task failed")

    @task
    def success():
        return True

    # task_identify_failure = identify_failure_type(dbt_exit_code=task_dbt.output)
    # task_dbt = dbt()

    task_identify_failure = identify_failure_type(dbt_exit_code=task_dbt.output)
    task_identify_failure >> [dbt_failure(), infra_failure(), success()]
