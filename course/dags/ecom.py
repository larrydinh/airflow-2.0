from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration



"""
1: dags .py file should be similar to function name
2: run pipeline everyday at midnight
3: CRON expression for schedule = '' https://crontab.guru/ 

"""

@dag(start_date = datetime(2025,1,1),
     schedule = '@daily',
     catchup = True, #only 1 DAG run for True and False. catchup look at the lasttime Dg was triggered->today
     description ="This DAG processes ecommerce data",
     tags =["team_a","ecom"],
     default_args={"retries":1},
     dagrun_timeout = duration(minutes =20)
    #  max_consecutive_failed_dag_runs_per_dag =2
    #  max_active_runs =1 #one dag can run at a time
)

def ecom():
    # ta = PythonOperator(task_id = 'ta')
    # tb = PythonOperator(task_id = 'tb')
    # tc = PythonOperator(task_id = 'tc')
    ta = EmptyOperator(task_id = 'ta')

ecom()