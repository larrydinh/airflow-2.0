from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2023,1,1),
    schedule='@daily',
    # schedule='0 0 * * *',  # every day at midnight
    catchup= False, #always trigger the last diagram
    tags = ['stock_market']
)
def stock_market():
    # @task.sensor()
    pass

stock_market()