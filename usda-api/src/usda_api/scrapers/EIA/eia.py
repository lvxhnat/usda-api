import pandas as pd
import requests
import json
import os
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, MetaData, PrimaryKeyConstraint, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
class EIA:
    def __init__(self):
        self.endpoint = "https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=pet&s=w_epooxe_yop_nus_mbbld&f=w"
        self.engine = create_engine('postgresql://postgres:Password*1@35.239.18.20/jy')
        
    def generate_request(self):
        info = requests.get(self.endpoint)
        return info
    
    def extract_eia(self):
        request_obj = self.generate_request()
        reponse = request_obj.text
        soup = BeautifulSoup(reponse, 'lxml')
        return soup
    
    def transform_eia(self, soup):
        data = []
        table_body = soup.find('tbody')

        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])

        columns = ['year-month', 'week1_enddate', 'week1_value', 'week2_enddate', 'week2_value',
                'week3_enddate', 'week3_value', 'week4_enddate', 'week4_value', 'week5_enddate', 'week5_value']
        ethanol = pd.DataFrame(data, columns=columns).dropna(
            subset=['year-month'])

        ethanol['year'] = ethanol['year-month'].apply(
            lambda x: x.split("-")[0])
        ethanol['month'] = ethanol['year-month'].apply(
            lambda x: x.split("-")[1])

        weeks_col = ['week1_enddate', 'week2_enddate',
                    'week3_enddate', 'week4_enddate', 'week5_enddate']

        data = []
        for week in weeks_col:
            ethanol[week] = ethanol[week].apply(
                lambda x: "-".join(x.split("/")) if x != None else x)
            ethanol[week] = ethanol['year'] + "-" + ethanol[week]
            ethanolItem = ethanol[[week, week.replace("enddate", "value")]]
            data.append(ethanolItem.rename(
                columns={week.replace("_enddate", "_value"): "value", week: "date"}))

        fuelethanol_production = pd.concat(
            data).dropna().sort_values("date")
        df = fuelethanol_production.applymap(lambda x : x.replace(',', ''))
        df.reset_index(drop= True, inplace = True)
        # os.makedirs('extract_eia', exist_ok = True)
        # file_name = 'extract_eia/extract_eia.csv'
        # df.to_csv(file_name, index= False)
        return df
    
    ###EXTRACT AND TRANSFORM 
    def extract_and_transform_eia(self):
        soup = self.extract_eia()
        df = self.transform_eia(soup)
        os.makedirs('./data/transform_eia', exist_ok= True)
        file_name = './data/transform_eia/transform_eia.csv'
        df.rename(columns = {'date':'Date', 'value' : 'Value'}, inplace = True)
        df.to_csv(file_name, index = False)
        return file_name
    def extract_and_transform_eia_weekly(self):
        soup = self.extract_eia()
        df = self.transform_eia(soup)
        df = df.tail(1)
        os.makedirs('./data/transform_eia_weekly', exist_ok= True)
        file_name = './data/transform_eia_weekly/transform_eia_weekly.csv'
        df.rename(columns = {'date':'Date', 'value' : 'Value'}, inplace = True)
        df.to_csv(file_name, index = False)
        return file_name
        
    ###LOAD
    def load_eia(self, path):
        metadata = MetaData()
        columns = [
            Column('Date', Date, primary_key = True),
            Column('Value', String),
        ]

        table = Table('eia', metadata, *columns, PrimaryKeyConstraint('Date'))
        
        metadata.create_all(self.engine)

        data_frame = pd.read_csv(path, parse_dates=["Date"], index_col=False)
        data_frame.Date = pd.to_datetime(data_frame.Date, utc = True)
        data_frame.Date = data_frame.Date.dt.date
        data_frame.to_sql('eia', self.engine, if_exists='append', index =False)
        return 'eia'
    
    def load_eia_weekly(self, path):
        metadata = MetaData()
        table = Table('eia', metadata, autoload=True, autoload_with=self.engine)
        data_frame = pd.read_csv(path, parse_dates=["Date"], index_col=False)
        data_frame.Date = pd.to_datetime(data_frame.Date, utc = True)
        data_frame.Date = data_frame.Date.dt.date
        data_to_insert = data_frame.to_dict('records')
        with self.engine.connect() as conn:
            with conn.begin():
                for data in data_to_insert:
                    upsert_stmt = insert(table).values(**data).\
                        on_conflict_do_update(
                            index_elements= ['Date'],
                            set_={c.name: c for c in insert(table).excluded if c.name not in ['Date']}
                        )
                    conn.execute(upsert_stmt)
        return 'eia'
    
    def query_eia(self):
        #metadata = MetaData()
        #metadata = MetaData(bind = self.engine)
        
        # yfinance_table = Table('yfinance', metadata, autoload = True, autoload_with=self.engine)
        # statement = select([yfinance_table.c.Open, yfinance_table.c.Close, yfinance_table.c.High, yfinance_table.c.Low, yfinance_table.c.Volume]) \
        #     .where(yfinance_table.c.Ticker == 'Corn')
        yfinance_table = self.engine.execute(  """
                                             SELECT "Date","Value" From eia
                                             """)
        df = pd.DataFrame(yfinance_table.fetchall(), columns=yfinance_table.keys())
        os.makedirs('./data/Ethanol_quote', exist_ok=True)
        df.to_csv('./data/Ethanol_quote/eia_quote.csv', index = False)
        return './data/Ethanol_quote/eia_quote.csv'
    


if __name__ == '__main__':
    eia = EIA()
    info = eia.extract_and_transform_eia_weekly()
    # df = pd.read_csv(path)
    # print(df.head())