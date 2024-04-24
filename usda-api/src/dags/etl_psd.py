from airflow.decorators import dag, task
from datetime import timedelta, datetime
import pandas as pd
import os
from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, MetaData, PrimaryKeyConstraint, inspect
from sqlalchemy.dialects.postgresql import insert
from copy import deepcopy


from usda_api.scrapers.psd.main import PSD

USDA_FAS_API_KEY_0="f5458abd-198d-402b-b75e-3ce48527b0d2"
psd = PSD(USDA_FAS_API_KEY_0)

default_args = {
    'owner' : 'xy',
    'start_date' : datetime(2024, 1, 1),
    'retries' : 0,
    'retry_delay' : timedelta(minutes=0.5)
}

@dag(dag_id='is3107_psd_v3', default_args = default_args, schedule = None, catchup = False, tags=['is3107'])
def etl_psd():
    @task(task_id = 'extract_general_information', multiple_outputs=True)
    def extract_general_information():
        info = {
            'commodity_dict': psd.extract_commodities_data(),
            'country_dict': psd.extract_countries_data(),
            'unitsOfMeasure': psd.extract_unitsOfMeasure(),
            'commodityAttributes': psd.extract_commodityAttributes()
        }
        return info

    @task(task_id = 'get_commodity_data_raw')
    def get_commodity_data_raw(commodity_dict, target_commodity, country_dict, 
                               target_country, market_year_start, market_year_end):
        commodity_code = commodity_dict[target_commodity]
        country_code = country_dict[target_country]["country_code"]  
        years = [str(year) for year in range(int(market_year_start), int(market_year_end)+1)]
        data = []
        for year in years:
            df = pd.DataFrame(psd.get_psd_commodities_data(commodity_code, country_code, year))
            if len(df) != 0:
                data.append(df)
        merged_data = pd.concat(data, axis=0, ignore_index=True)
        return merged_data      

    @task(task_id = 'get_commodity_data_ts')
    def get_commodity_data_ts(commdity_data_raw, unitsOfMeasure, commodityAttributes):
        df1 = pd.merge(commdity_data_raw, unitsOfMeasure, on='unit_id', how='inner')
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
        path = './data/extract_psd/extract_psd.csv'
        data.to_csv(path, index=False)
        return data
    
    
    @task(task_id = 'create_psd_table')
    def create_psd_table(engine, psd_ts):
        metadata = MetaData()
        columns = [
            Column('Date', Date, primary_key=True),
            Column('Commodity_Code', String),
            Column('Country_Code', String),
            Column('Area_Harvested_1000_HA', Float),
            Column('Beginning_Stocks_1000_MT', Float),
            Column('Domestic_Consumption_1000_MT', Float),
            Column('Ending_Stocks_1000_MT', Float),
            Column('Exports_1000_MT', Float),
            Column('FSI_Consumption_1000_MT', Float),
            Column('Feed_Dom_Consumption_1000_MT', Float),
            Column('Imports_1000_MT', Float),
            Column('Production_1000_MT', Float),
            Column('TY_Exports_1000_MT', Float),
            Column('TY_Imp_from_US_1000_MT', Float),
            Column('TY_Imports_1000_MT', Float),
            Column('Total_Distribution_1000_MT', Float),
            Column('Total_Supply_1000_MT', Float),
            Column('Yield_MT_per_HA', Float)
        ]
        table = Table('psd', metadata, *columns, PrimaryKeyConstraint('Date'))
        metadata.create_all(engine)
        return True

    @task(task_id='load_psd_data')
    def load_psd_data(psd_data, engine, table_created):
        data = deepcopy(psd_data)
        data.to_sql(name='psd', con=engine, if_exists='append', index=False)
        return "PSD historical inserted"

    info = extract_general_information()
    commodity_dict = info['commodity_dict']
    country_dict = info['country_dict']
    unitsOfMeasure = info['unitsOfMeasure']
    commodityAttributes = info['commodityAttributes']

    commodity_data_raw = get_commodity_data_raw(commodity_dict=commodity_dict,
                                                target_commodity='Corn',
                                                country_dict=country_dict,
                                                target_country='United States',
                                                market_year_start='2000',
                                                market_year_end='2024')
    commodity_data_ts = get_commodity_data_ts(commdity_data_raw=commodity_data_raw,
                                              unitsOfMeasure=unitsOfMeasure,
                                              commodityAttributes=commodityAttributes)


    engine = create_engine('postgresql://postgres:Password*1@35.239.18.20/is3107')
    table_created = create_psd_table(engine, commodity_data_ts)
    load_psd_data(psd_data=commodity_data_ts,
                    engine=engine,
                    table_created=table_created)


taskflow_dag = etl_psd()