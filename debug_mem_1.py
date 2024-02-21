from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from kubernetes.client import V1ResourceRequirements

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'celery_k8s_dag',
    default_args=default_args,
    description='A simple DAG using CeleryKubernetesExecutor',
    schedule_interval=timedelta(days=1),  # Set the desired schedule interval
)

def dummy_math_task(task_id, x, y):
    result = x + y
    print(f'Task {task_id} result: {result}')

with dag:
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=dummy_math_task,
        op_args=['task_1', 5, 10],
        executor_config={
            "KubernetesExecutor": {"request_memory": V1ResourceRequirements(requests={"memory": "64Mi"})}
        },
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=dummy_math_task,
        op_args=['task_2', 15, 20],
        executor_config={
            "KubernetesExecutor": {"request_memory": V1ResourceRequirements(requests={"memory": "128Mi"})}
        },
    )

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=dummy_math_task,
        op_args=['task_3', 25, 30],
        executor_config={
            "KubernetesExecutor": {"request_memory": V1ResourceRequirements(requests={"memory": "256Mi"})}
        },
    )

    task_4 = PythonOperator(
        task_id='task_4',
        python_callable=dummy_math_task,
        op_args=['task_4', 35, 40],
        executor_config={
            "KubernetesExecutor": {"request_memory": V1ResourceRequirements(requests={"memory": "512Mi"})}
        },
    )

    # Set task dependencies
    task_1 >> task_2 >> task_3 >> task_4
