from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def _task_a():
    print("Task A")
    return 42

def _task_b(ti=None):
    print("Task B")
    print(ti.xcom_pull(task_ids='task_a')) #xcom: share data between the tasks

with DAG(
    dag_id="without_taskflow_api",
    start_date=datetime(2023,1,1),
    schedule='@daily',
    # schedule='0 0 * * *',  # every day at midnight
    catchup= False, #always trigger the last diagram
    tags = ['taskflow']
):
    task_a = PythonOperator(
        task_id ='task_a',
        python_callable= _task_a,
    )
    task_b = PythonOperator(
        task_id ='task_b',
        python_callable= _task_b,
    )
    task_a >>task_b