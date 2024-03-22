from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.usda_api.scrapers.WEATHER.weather import WEATHER


default_args = {
    'owner': 'jy',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=0.1)  # Changed 'retry_interval' to 'retry_delay'
}

@dag(dag_id='etl_weather', default_args=default_args, catchup=False, schedule=None)
def etl_weather():

    @task(task_id=f'extract_weather')
    def extract_weather():
        weather = WEATHER()
        path = weather.extract_weather()
        return path
    @task(task_id=f'transform_weather')
    def transform_weather(path):
        weather = WEATHER()
        path = weather.transform_weather(path)
        return path
    @task(task_id=f'load_weather')
    def load_weather(path):
        weather = WEATHER()
        weather.load_weather(path)
        return 
        #y_finance_ticker = y_finance_ticker.load_ticker()
        #return
        # Define task flow for each ticker
       
    path = extract_weather()
    path2 = transform_weather(path)
    load_weather(path2)

etl_weather_dag = etl_weather()