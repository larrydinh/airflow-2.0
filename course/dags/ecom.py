from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from pendulum import datetime, timedelta


"""
1: dags .py file should be similar to function name
2: run pipeline everyday at midnight
3: CRON expression for schedule = '' https://crontab.guru/ 

"""

@dag(start_date = datetime(2025,1,1),
     schedule = '@daily',
     catchup = False
     description:"This DAG processes ecommerce data",
     tags =["team_a","ecom"],
     default_args={"retries":1},
     dagrun_timeout = timedelta(minutes =20),
     max_consecutive_failed_dag_run =2
     max_active_runs =1 #one dag can run at a time
)

def ecom():
    ta = PythonOperator(task_id = 'ta')
    tb = PythonOperator(task_id = 'tb')
    tc = PythonOperator(task_id = 'tc')

ecom()