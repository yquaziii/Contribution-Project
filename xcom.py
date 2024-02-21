from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define a function to perform addition
def perform_addition(**kwargs):
    # Get the context and task instance
    ti = kwargs['ti']

    # Perform addition
    result = 5 + 7

    # Log the result
    print(f"addition Result: {result}")

    # Set the result in XCom
    ti.xcom_push(key='addition_result', value=result)



# Define a function to perform multiplication
def perform_multiplication(**kwargs):
    # Get the context and task instance
    ti = kwargs['ti']

    # Retrieve the addition result from XCom
    addition_result = ti.xcom_pull(key='addition_result', task_ids='addition_task')

    # Perform multiplication
    result = addition_result * 10

    # Log the result
    print(f"Multiplication Result: {result}")

# Define the DAG
dag = DAG(
    'example_xcom_dag',
    schedule_interval=None,  # You can set a schedule here if needed
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Perform Addition
addition_task = PythonOperator(
    task_id='addition_task',
    python_callable=perform_addition,
    provide_context=True,
    dag=dag,
)

# Task 2: Perform Multiplication
multiplication_task = PythonOperator(
    task_id='multiplication_task',
    python_callable=perform_multiplication,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
addition_task >> multiplication_task
