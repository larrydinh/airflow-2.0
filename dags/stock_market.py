from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

from include.stock_market.tasks import _get_stock_prices, _store_prices
SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2023,1,1),
    schedule='@daily',
    # schedule='0 0 * * *',  # every day at midnight
    catchup= False, #always trigger the last diagram
    tags = ['stock_market']
)
def stock_market():
    @task.sensor(poke_interval =30, timeout=300, mode = 'poke')
    #this def use to detect whether the API is available or not. If API url return null after called then API is available
    #STEP1: CHECK IF API IS AVAILABLE OR NOT
    def is_api_available() -> PokeReturnValue:
        import requests
        api = BaseHook.get_connection('stock_api') #  Gets Airflow connection details
        url = f"{api.host}{api.extra_dejson['endpoint']}"  # Constructs full URL
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers']) # Sends GET request
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done = condition, xcom_value = url)

    #use traditional PythonOperator here since it will be tricky when decorator working with Docker operators as well.
    #STEP2: GET STOCK PRICES FROM API
    get_stock_prices = PythonOperator(
    task_id='get_stock_prices',
    python_callable=_get_stock_prices,
    op_kwargs={'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL}
    #{{task}}: task between 2 curly bracket will only be evaluated when the task run
    )
    #STEP3: STORE DATA IN MINIO
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
    )
    #use DockerOperator to open Docker container after built images by docker build -t airflor/stock app . 
    #STEP4: FORMAT DATA USING SPARK
    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        }
    


    )



    is_api_available() >> get_stock_prices >> store_prices >> format_prices

stock_market()