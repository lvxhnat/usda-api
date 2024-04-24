
import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import date, timedelta
import os
# from google.cloud import bigquery
# from google.oauth2 import service_account
# import pandas_gbq

from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, MetaData, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import insert

class WEATHER:
    def __init__(self):
        self.states_coordinates = {
            "Iowa": ["41.8780","-93.0977"], 
            "Illinois": ["40.6331", "-89.3985"], 
            "South Dakota": ["43.9695","-99.9018"],
            "Nebraska": ["41.4925", "-99.9018"], 
            "Minnesota" : ['46.7296', '-94.6859'], 
            "Indiana" : ['40.5512', '-85.6024'],
        }
        
        self.data_frame = pd.DataFrame()
        #self.credentials = service_account.Credentials.from_service_account_file('/Users/jiayuan/Desktop/project/usda-api/src/usda_api/scrapers/google.json')
        #self.project_id = 'airflow-418412'
        self.engine = create_engine('postgresql://postgres:Password*1@35.239.18.20/is3107')
        
    # Setup the Open-Meteo API client with cache and retry on error
    def extract(self, start,dir):
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 1, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)
        os.makedirs(dir, exist_ok = True)
        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://archive-api.open-meteo.com/v1/archive"
        for key, value in self.states_coordinates.items():
            params = {
                "latitude": value[0],
                "longitude": value[1],
                "start_date": start,
                "end_date": date.today().strftime("%Y-%m-%d"),
                "daily": [ "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "precipitation_sum"]
            }
            responses = openmeteo.weather_api(url, params=params)

            # Process first location. Add a for-loop for multiple locations or weather models
            response = responses[0]

            # Process daily data. The order of variables needs to be the same as requested.
            daily = response.Daily()
            daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
            daily_temperature_2m_mean = daily.Variables(2).ValuesAsNumpy()
            daily_precipitation_sum = daily.Variables(3).ValuesAsNumpy()

            daily_data = {"Date": pd.date_range(
                start = pd.to_datetime(daily.Time(), unit = "s", utc = False),
                end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = False),
                freq = pd.Timedelta(seconds = daily.Interval()),
                inclusive = "left",
                ),
                "State" : key,
                #"longitude" : value[0],
                #"latitude" : value[1],
                "Temperature_2m_max" : daily_temperature_2m_max,
                "Temperature_2m_min" : daily_temperature_2m_min,
                "Temperature_2m_mean" : daily_temperature_2m_mean,
                "Precipitation_sum" : daily_precipitation_sum
                }
            daily_dataframe = pd.DataFrame(data = daily_data)
            self.data_frame = pd.concat([self.data_frame, daily_dataframe], ignore_index = True)
        self.data_frame.to_csv(f'{dir}/extract_weather.csv', index =  False)
        return f'{dir}/extract_weather.csv'
    
    def extract_weather(self):
        dir = './data/extract_weather'
        start = "2010-01-01"
        path = self.extract(start, dir)
        return path
    
    def extract_weather_weekly(self):
        dir = './data/extract_weather_weekly'
        start = date.today().strftime('%Y-%m-%d')
        path = self.extract(start, dir)
        return path
    
    
    
    def transform_weather(self, path):
        dir = './data/transform_weather'
        os.makedirs(dir, exist_ok = True)
        df = pd.read_csv(path)
        df.date = pd.to_datetime(df.date, utc = False)
        df.set_index('Date', inplace = True)
        df_groups = df.groupby('State')
        for state, group in df_groups:
            df_transform =  group[['Temperature_2m_max',
       'Temperature_2m_min', 'Temperature_2m_mean', 'Precipitation_sum']]
            df_weekly = df_transform.resample('W-MON').mean()
            df_monthly = df_transform.resample('M').mean()
            df_weekly.to_csv(f'{dir}/{state}_weekly_weather.csv', index =False)
            df_monthly.to_csv(f'{dir}/{state}_monthly_weather.csv', index = False)
        return dir
        #print(df.head())
        
    '''def load_weather_old(self, path):
        list_dir = os.listdir(path)
        schema = [
            {'name' : 'date', 'type': 'DATE'},
            {'name' : 'temperature_2m_max', 'type' : 'FLOAT64'},
            {'name' : 'temperature_2m_min', 'type' : 'FLOAT64'},
            {'name' : 'temperature_2m_mean', 'type' : 'FLOAT64'},
            {'name' : 'precipitation_sum', 'type' : 'FLOAT64'},
        ]
        list_table_name = []
        for i in list_dir:
            data_frame = pd.read_csv(f'{path}/{i}', index_col=False)
            name = i[:i.rfind('.')]
            list_table_name.append(name)
            table_name = f"{self.project_id}.database.{name}"
            #pandas_gbq.to_gbq(dataframe=data_frame, destination_table = table_name, project_id = self.project_id, credentials = self.credentials, if_exists = 'replace',table_schema = schema)
        return list_table_name'''
    
    def load_weather(self, path):
        metadata = MetaData()
        columns = [
            Column('State', String, primary_key = True),
            Column('Date', Date, primary_key = True),
            Column('Temperature_2m_min', Float),
            Column('Temperature_2m_max', Float),
            Column('Temperature_2m_mean', Float),
            Column('Precipitation_sum', Float)
        ]
        table = Table('weather', metadata, *columns, PrimaryKeyConstraint('State', 'Date'))
        metadata.create_all(self.engine)
        
        data_frame = pd.read_csv(path, parse_dates = ['Date'], index_col=False)
        data_frame['Date'] = pd.to_datetime(data_frame['Date'], utc =False)
        data_frame['Date'] = data_frame.Date.dt.date
        data_frame.to_sql('weather', self.engine, if_exists='append', index=False)
        return 'weather'
    
    def load_weather_weekly(self, path):
        metadata = MetaData()
        table = Table('weather', metadata, autoload=True, autoload_with = self.engine)
        data_frame = pd.read_csv(path, parse_dates = ['Date'], index_col=False)
        data_frame['Date'] = pd.to_datetime(data_frame['Date'], utc = False)
        data_frame['Date'] = data_frame.Date.dt.date
        data_to_insert = data_frame.to_dict('records')
        with self.engine.connect() as conn:
            with conn.begin():
                for data in data_to_insert:
                    upsert_stmt = insert(table).values(**data).\
                        on_conflict_do_update(
                            index_elements= ['State', 'Date'],
                            set_={c.name: c for c in insert(table).excluded if c.name not in ['State', 'Date']}
                        ) 
                    conn.execute(upsert_stmt)
        return 'weather'
    
    def transform_weather_analysis(self):

        weekly_temperature = self.engine.execute("""
                                            SELECT "State", "Date", "Temperature_2m_mean","Temperature_2m_min", "Temperature_2m_max", "Precipitation_sum"
                                            FROM weather WHERE "Date" BETWEEN (NOW() - INTERVAL '1 year')::DATE AND NOW()::DATE
                                            """) 
        
        df = pd.DataFrame(weekly_temperature.fetchall(), columns=weekly_temperature.keys())
        os.makedirs('./data/Weather_quote', exist_ok= True)
        df.to_csv('./data/Weather_quote/weather_quote.csv', index = False)
        return'./data/Weather_quote/weather_quote.csv'
        

if __name__ == "__main__":
    weather = WEATHER()
    path = weather.extract_weather_weekly()
    ##path2 = weather.transform_weather(path)
    print(weather.load_weather_weekly(path))