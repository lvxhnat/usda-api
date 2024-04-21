from datetime import datetime, timedelta
from airflow.decorators import dag, task
#from yFinance2 import yFinance2

from usda_api.scrapers.yFinance2.yFinance2 import yFinance2
from usda_api.scrapers.WEATHER.weather import WEATHER
from usda_api.scrapers.EIA.eia import EIA
from usda_api.scrapers.ESR.esr import ESR

default_args = {
    'owner': 'jy',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=0.1)  # Changed 'retry_interval' to 'retry_delay'
}

@dag(dag_id='etl_final', default_args=default_args, catchup=False, schedule='@weekly')
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

    @task(task_id='extract_and_transform_eia_weekly')
    def extract_and_transform_eia_weekly():
        eia = EIA()
        return eia.extract_and_transform_eia_weekly()
    @task(task_id = 'load_eia_weekly')
    def load_eia_weekly(path):
        eia = EIA()
        return eia.load_eia_weekly(path)
    
    @task(task_id='extract_and_transform_esr_weekly')
    def extract_and_transform_esr_weekly():
        esr = ESR()
        return esr.extract_and_transform_esr_weekly()
    @task(task_id = 'load_esr_weekly')
    def load_esr_weekly(path):
        esr = ESR()
        return esr.load_esr_weekly(path)


    @task(task_id='query_yfinance_corn_analysis')
    def transform_yfinance_corn(path):
        yfinance = yFinance2()
        path = yfinance.transform_ticker_corn()
        return path
    @task(task_id='query_yfinance_oil_analysis')
    def transform_yfinance_oil(path):
        yfinance = yFinance2()
        path = yfinance.transform_ticker_oil()
        return path
    @task(task_id='query_yfinance_usd_analysis')
    def transform_yfinance_usd(path):
        yfinance = yFinance2()
        path = yfinance.transform_ticker_usd()
        return path
    @task(task_id='query_yfinance_gas_analysis')
    def transform_yfinance_gas(path):
        yfinance = yFinance2()
        path = yfinance.transform_ticker_gas()
        return path

    @task(task_id='query_weather_analysis')
    def transform_weather_analysis(path):
        weather = WEATHER()
        path = weather.transform_weather_analysis()
        return path
    @task(task_id='query_ethanol_analysis')
    def transform_ethanol_analysis(path):
        eia = EIA()
        path = eia.query_eia()
        return path
    @task(task_id ='query_export_analysis')
    def transform_export_analysis(path):
        esr = ESR()
        path = esr.query_esr_all()
        path2 = esr.query_esr_yearly()
        return path

    @task(task_id='end')
    def query(*args):
        for i in args:
            print(i)
    
    path_yfinance = extract_yfinance_weekly()
    path2_yfinance = transform_yfinance_weekly(path_yfinance)
    path3_yfinance = load_yfinance_weekly(path2_yfinance)

    path_weather = extract_weather_weekly()
    path2_weather = load_weather_weekly(path_weather)
   
    path_eia = extract_and_transform_eia_weekly()
    path2_eia = load_eia_weekly(path_eia)
    
    path_esr = extract_and_transform_esr_weekly()
    path2_esr = load_esr_weekly(path_esr)

    path_yfinance_corn = transform_yfinance_corn(path3_yfinance)
    path_yfinance_oil = transform_yfinance_oil(path3_yfinance)
    path_yfinance_usd = transform_yfinance_usd(path3_yfinance)
    path_yfinance_gas = transform_yfinance_gas(path3_yfinance)
    
    path_weather_analysis = transform_weather_analysis(path2_weather)
    path_ethanol_analysis = transform_ethanol_analysis(path2_eia)
    path_export_analysis = transform_export_analysis(path2_esr)
    #analyse_yfinance_corn(path_yfinance_corn)
    
    query(path_yfinance_corn,path_yfinance_oil, path_yfinance_usd, path_yfinance_gas, path_weather_analysis, path_ethanol_analysis, path_export_analysis)

etl_dag = etl_final()