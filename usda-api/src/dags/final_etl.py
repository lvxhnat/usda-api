from datetime import datetime, timedelta
from airflow.decorators import dag, task
#from yFinance2 import yFinance2

from usda_api.scrapers.yFinance2.yFinance2 import yFinance2
from usda_api.scrapers.WEATHER.weather import WEATHER


default_args = {
    'owner': 'jy',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=0.1)  # Changed 'retry_interval' to 'retry_delay'
}

@dag(dag_id='etl_final', default_args=default_args, catchup=False, schedule=None)
def etl_final():
    
    @task(task_id=f'extract_yfinance_weekly')
    def extract_yfinance_weekly():
        yfinance_ticker = yFinance2()
        path = yfinance_ticker.extract_ticker_weekly()
        return path

    @task(task_id=f'transform_yfinance_weekly')
    def transform_yfinance_weekly(path):
        yfinance_ticker = yFinance2()
        path2 = yfinance_ticker.transform_ticker_weekly(path)
        return path2

    @task(task_id=f'load_yfinance_weekly')
    def load_yfinance_weekly(path):
        yfinance_ticker = yFinance2()
        return yfinance_ticker.load_ticker_weekly(path)
    
    @task(task_id='extract_weather_weekly')
    def extract_weather_weekly():
        weather = WEATHER()
        return weather.extract_weather_weekly()

    @task(task_id='load_weather_weekly')
    def load_weather_weekly(path):
        weather= WEATHER()
        return weather.load_weather_weekly(path)
    
    @task(task_id='transform_yfinance_corn')
    def transform_yfinance_corn(path):
        yfinance = yFinance2()
        path = yfinance.transform_ticker_corn()
        return path
    # @task(task_id='analyse_yfinance_corn')
    # def analyse_yfinance_corn(path):
    #     ml = MachineLearning(path)
    #     ml.gradio()
    #     return
    
    @task(task_id='transfrom_weather_analysis')
    def transform_weather_analysis(path):
        weather = WEATHER()
        path = weather.transform_weather_analysis()
        return path

    @task(task_id='query')
    def query(*args):
        for i in args:
            print(i)
    
    path_yfinance = extract_yfinance_weekly()
    path2_yfinance = transform_yfinance_weekly(path_yfinance)
    path3_yfinance = load_yfinance_weekly(path2_yfinance)

    path_weather = extract_weather_weekly()
    path2_weather = load_weather_weekly(path_weather)
    
    path_yfinance_corn = transform_yfinance_corn(path3_yfinance)
    path_weather_analysis = transform_weather_analysis(path2_weather)
    #analyse_yfinance_corn(path_yfinance_corn)

    query(path3_yfinance, path2_weather)

etl_dag = etl_final()