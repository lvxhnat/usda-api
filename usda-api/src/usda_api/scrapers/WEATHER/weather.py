
import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import date, timedelta
import os
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
    
    
    # Setup the Open-Meteo API client with cache and retry on error
    def extract_weather(self):
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 1, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)
        os.makedirs('extract_weather', exist_ok = True)
        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://archive-api.open-meteo.com/v1/archive"
        for key, value in self.states_coordinates.items():
            params = {
                "latitude": value[0],
                "longitude": value[1],
                "start_date": "2010-01-01",
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

            daily_data = {"date": pd.date_range(
                start = pd.to_datetime(daily.Time(), unit = "s", utc = False),
                end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = False),
                freq = pd.Timedelta(seconds = daily.Interval()),
                inclusive = "left",
                ),
                "state" : key,
                "longitude" : value[0],
                "latitude" : value[1],
                "temperature_2m_max" : daily_temperature_2m_max,
                "temperature_2m_min" : daily_temperature_2m_min,
                "temperature_2m_mean" : daily_temperature_2m_mean,
                "precipitation_sum" : daily_precipitation_sum
                }
            daily_dataframe = pd.DataFrame(data = daily_data)
            self.data_frame = pd.concat([self.data_frame, daily_dataframe], ignore_index = True)
        self.data_frame.to_csv('extract_weather/extract_weather.csv')
        return 'extract_weather/extract_weather.csv'
    
    def transform_weather(self, path):
        dir = 'transform_weather'
        os.makedirs(dir, exist_ok = True)
        df = pd.read_csv(path)
        df.drop(columns='Unnamed: 0', inplace=True)
        df.date = pd.to_datetime(df.date, utc = False)
        df.set_index('date', inplace = True)
        df_groups = df.groupby('state')
        for state, group in df_groups:
            df_transform =  group[['temperature_2m_max',
       'temperature_2m_min', 'temperature_2m_mean', 'precipitation_sum']]
            df_weekly = df_transform.resample('W-MON').mean()
            df_monthly = df_transform.resample('M').mean()
            df_weekly.to_csv(f'{dir}/{state}_weekly_weather.csv')
            df_monthly.to_csv(f'{dir}/{state}_monthly_weather.csv')
        return dir
        #print(df.head())
        
    def load_weather(self, path):
        print('success')
        return