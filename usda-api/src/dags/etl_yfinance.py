from datetime import datetime, timedelta
from airflow.decorators import dag, task
#from yFinance2 import yFinance2
#import sys
#sys.path.append('../yFinance2')
from src.usda_api.scrapers.yFinance2.yFinance2 import yFinance2
#TICKERS = {
#    'ZC_F': 'ZC=F',
#    'CL_F': 'CL=F',
#    'NG_F': 'NG=F',
#    'DX_Y_NYB': 'DX-Y.NYB',
#}

default_args = {
    'owner': 'jy',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=0.1)  # Changed 'retry_interval' to 'retry_delay'
}

@dag(dag_id='etl_yfinance', default_args=default_args, catchup=False, schedule=None)
def etl_yfinance():
    '''for ticker_name, ticker_symbol in TICKERS.items():
        global y_finance_ticker 
        y_finance_ticker = yFinance2(ticker_symbol) '''
    @task(task_id=f'extract_yfinance')
    def extract_yfinance():
        yfinance_ticker = yFinance2()
        path = yfinance_ticker.extract_ticker()
        return path
        #y_finance_ticker = y_finance_ticker.extract_ticker()
        #return 1

    @task(task_id=f'transform_yfinance')
    def transform_yfinance(path):
        yfinance_ticker = yFinance2()
        path2 = yfinance_ticker.transform_ticker(path)
        return path2
        #y_finance_ticker = y_finance_ticker.transform_ticker()
        #return 2

    @task(task_id=f'load_yfinance')
    def load_yfinance(path):
        yfinance_ticker = yFinance2()
        return yfinance_ticker.load_ticker(path)
        #y_finance_ticker = y_finance_ticker.load_ticker()
        #return
        # Define task flow for each ticker
        
    path = extract_yfinance()
    path2 = transform_yfinance(path)
    load_yfinance(path2)

etl_yfinance_dag = etl_yfinance()