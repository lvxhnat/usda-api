from datetime import datetime, timedelta, date
from airflow.decorators import dag, task
import pandas as pd
from copy import deepcopy
import os
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.dialects.postgresql import insert

from usda_api.scrapers.yFinance2.yFinance2 import yFinance2
from usda_api.scrapers.WEATHER.weather import WEATHER
from usda_api.scrapers.EIA.eia import EIA
from usda_api.scrapers.ESR.esr import ESR
from usda_api.scrapers.psd.main import PSD

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

    @task(task_id = 'get_commodity_data_raw_weekly')
    def get_commodity_data_raw_weekly(target_commodity, target_country):
        commodity_dict = psd.extract_commodities_data()
        country_dict = psd.extract_countries_data()
        commodity_code = commodity_dict[target_commodity]
        country_code = country_dict[target_country]["country_code"]  
        curr_year = str(date.today().year - 1)
        df = pd.DataFrame(psd.get_psd_commodities_data(commodity_code, country_code, curr_year))
        return df     

    @task(task_id = 'get_commodity_data_ts_weekly')
    def get_commodity_data_ts_weekly(commodity_data_raw):
        unitsOfMeasure = psd.extract_unitsOfMeasure()
        commodityAttributes = psd.extract_commodityAttributes()
        df1 = pd.merge(commodity_data_raw, unitsOfMeasure, on='unit_id', how='inner')
        df2 = pd.merge(df1, commodityAttributes, on='attribute_id', how='inner')
        df2['date'] = pd.to_datetime(df2['market_year']+'-'+df2['month'], format='%Y-%m')
        df2['attribute_name_unit'] = df2['attribute_name'] + ' ' + df2['unit_description']
        df3 = df2.pivot_table(index=['date', 'commodity_code', 'country_code'], 
                            columns='attribute_name_unit',
                            values='value').reset_index()
        df3 = df3.rename_axis(None, axis=1)
        #standardise data into format requried for insertion
        sql_table_cols = ['Date', 'Commodity_Code', 'Country_Code', 'Area_Harvested_1000_HA', 'Beginning_Stocks_1000_MT', 'Domestic_Consumption_1000_MT',
                    'Ending_Stocks_1000_MT', 'Exports_1000_MT', 'FSI_Consumption_1000_MT', 'Feed_Dom_Consumption_1000_MT', 'Imports_1000_MT', 'Production_1000_MT', 
                    'TY_Exports_1000_MT', 'TY_Imp_from_US_1000_MT', 'TY_Imports_1000_MT', 'Total_Distribution_1000_MT', 'Total_Supply_1000_MT', 'Yield_MT_per_HA']
        data = deepcopy(df3)
        data.columns = sql_table_cols
        data['Date'] = pd.to_datetime(data['Date'], utc =False)
        data['Date'] = data.Date.dt.date
        #save to csv and return the df
        os.makedirs('./data/extract_psd', exist_ok=True)
        path = './data/extract_psd/extract_psd_weekly.csv'
        data.to_csv(path, index=False)
        return data
    

    @task(task_id='load_psd_data_weekly')
    def load_psd_data_weekly(psd_data, engine):
        metadata = MetaData()
        table = Table('psd', metadata, autoload=True, autoload_with=engine)
        data_frame = deepcopy(psd_data)
        print(len(data_frame))
        data_to_insert = data_frame.to_dict('records')
        with engine.connect() as conn:
            with conn.begin(): 
                for data in data_to_insert:
                    #update and insert statement
                    upsert_stmt = insert(table).values(**data).\
                        on_conflict_do_update(
                            index_elements = ['Date'],
                            set_ = {c.name: c for c in insert(table).excluded if c.name not in ['Date']}
                        )
                    conn.execute(upsert_stmt)
        return "PSD weekly inserted"

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
    
    USDA_FAS_API_KEY_0="f5458abd-198d-402b-b75e-3ce48527b0d2"
    psd = PSD(USDA_FAS_API_KEY_0)
    engine = create_engine('postgresql://postgres:Password*1@35.239.18.20/is3107')
    commodity_data_raw = get_commodity_data_raw_weekly(target_commodity='Corn',
                                                       target_country='United States')
    commodity_data_ts = get_commodity_data_ts_weekly(commdity_data_raw=commodity_data_raw)
    load_weekly_psd = load_psd_data_weekly(psd_data=commodity_data_ts, engine=engine)
    
    query(path_yfinance_corn,path_yfinance_oil, path_yfinance_usd, path_yfinance_gas, path_weather_analysis, path_ethanol_analysis, path_export_analysis, load_weekly_psd)

etl_dag = etl_final()