from datetime import datetime
import pandas as pd
import numpy as np
import json
import yfinance as yf
import os
#from google.cloud import bigquery #
#from google.oauth2 import service_account #
#import pandas_gbq #
    
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import yfinance as yf
import os
from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, MetaData, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import insert
TICKER = {'ZC=F':'Corn', 'CL=F':'Oil', 'NG=F': 'Gas' , 'DX-Y.NYB' : 'USD'}
class yFinance2:
    def __init__(self):
        self.data =["Date", "Open", "High", "Low", "Close", "Volume"] 
        self.TICKER = TICKER
        #self.credentials = service_account.Credentials.from_service_account_file('/Users/jiayuan/Desktop/project/usda-api/src/usda_api/scrapers/google.json') #
        #self.project_id = 'airflow-418412' #
        self.engine = create_engine('postgresql://postgres:Password*1@35.239.18.20/jy')
    
    ##EXTRACT##
    def extract(self, start, dir):
        os.makedirs(dir, exist_ok=True)
        for t in self.TICKER.keys():
             path = f'{dir}/{t}.csv'
             info = yf.Ticker(t)
             info.history(start = start, end = datetime.now().strftime("%Y-%m-%d")).to_csv(path)
        return
                     
    def extract_ticker(self):
        dir = f'./data/extract_yfinance'
        self.extract("2000-08-30", dir)
        return dir
    
    def extract_ticker_weekly(self):
        dir = f'./data/extract_yfinance_weekly'
        start_date = str(datetime.now().date() - timedelta(days= 7))
        self.extract(start_date, dir)
        return dir
    
    
    
    ##TRANSFORM##
    def transform(self, path, dir):
        os.makedirs(dir, exist_ok = True)
        df_all = pd.DataFrame()       
        for t in self.TICKER.keys():
            path_temp = path + '/' + t + '.csv'
            data_frame = pd.read_csv(path_temp)
            df = data_frame[self.data]
            df['Ticker_name'] = self.TICKER[t]
            df_all = pd.concat([df_all, df], axis=0, ignore_index=True)
        path2 = dir + '/' + 'all' + '.csv'
        df_all.to_csv(path2, index = False)
        return path2
    
    def transform_ticker(self, path):
        dir = f'./data/transform_yfinance'
        path2 = self.transform(path, dir)
        return path2
    
    def transform_ticker_weekly(self, path):
        dir = f'./data/transform_yfinance_weekly'
        path2 = self.transform(path, dir)
        return path2
    
        
    ##LOAD##       
    def load_ticker(self, path):

        metadata = MetaData()
        columns = [
            Column('Ticker_name', String, primary_key=True),
            Column('Date', Date, primary_key = True),
            Column('Open', Float) ,
            Column('High', Float), 
            Column('Low', Float), 
            Column('Close', Float) ,
            Column('Volume', Integer)
        ]

        table = Table('yfinance', metadata, *columns, PrimaryKeyConstraint('Ticker_name', 'Date'))
        
        metadata.create_all(self.engine)

        data_frame = pd.read_csv(path, parse_dates=["Date"], index_col=False)
        data_frame.Date = pd.to_datetime(data_frame.Date, utc = True)
        data_frame.Date = data_frame.Date.dt.date
        data_frame.to_sql('yfinance', self.engine, if_exists='append', index =False)
        return 'yfinance'
    
    def load_ticker_weekly(self, path):
        metadata = MetaData()
        table = Table('yfinance', metadata, autoload=True, autoload_with=self.engine)
        data_frame = pd.read_csv(path, parse_dates=["Date"], index_col=False)
        data_frame.Date = pd.to_datetime(data_frame.Date, utc = True)
        data_frame.Date = data_frame.Date.dt.date
        data_to_insert = data_frame.to_dict('records')
        with self.engine.connect() as conn:
            with conn.begin():
                for data in data_to_insert:
                    upsert_stmt = insert(table).values(**data).\
                        on_conflict_do_update(
                            index_elements= ['Ticker_name', 'Date'],
                            set_={c.name: c for c in insert(table).excluded if c.name not in ['Ticker_name', 'Date']}
                        )
                    conn.execute(upsert_stmt)
        return 'yfinance'

    def transform_ticker_corn(self):
        #metadata = MetaData()
        #metadata = MetaData(bind = self.engine)
        
        # yfinance_table = Table('yfinance', metadata, autoload = True, autoload_with=self.engine)
        # statement = select([yfinance_table.c.Open, yfinance_table.c.Close, yfinance_table.c.High, yfinance_table.c.Low, yfinance_table.c.Volume]) \
        #     .where(yfinance_table.c.Ticker == 'Corn')
        yfinance_table = self.engine.execute(  """
                                             SELECT "Date", "Open", "Close", "High", "Low", "Volume" From yfinance WHERE "Ticker_name" ='Corn'
                                             """)
        df = pd.DataFrame(yfinance_table.fetchall(), columns=yfinance_table.keys())
        os.makedirs('./data/Corn_quote', exist_ok=True)
        df.to_csv('./data/Corn_quote/corn_quote.csv', index = False)
        return './data/Corn_quote/corn_quote.csv'
    
    def transform_ticker_oil(self):
        
        yfinance_table = self.engine.execute(  """
                                             SELECT "Date", "Open", "Close", "High", "Low", "Volume" From yfinance WHERE "Ticker_name" ='Oil'
                                             WHERE "Date" BETWEEN (NOW() - INTERVAL '1 year')::DATE AND NOW()::DATE
                                             """)
        df = pd.DataFrame(yfinance_table.fetchall(), columns=yfinance_table.keys())
        os.makedirs('./data/Oil_quote', exist_ok=True)
        df.to_csv('./data/Oil_quote/oil_quote.csv', index = False)
        return './data/Oil_quote/oil_quote.csv'
    
    def transform_ticker_usd(self):
    
        yfinance_table = self.engine.execute(  """
                                             SELECT "Date", "Open", "Close", "High", "Low", "Volume" From yfinance WHERE "Ticker_name" ='USD'
                                             WHERE "Date" BETWEEN (NOW() - INTERVAL '1 year')::DATE AND NOW()::DATE
                                             """)
        df = pd.DataFrame(yfinance_table.fetchall(), columns=yfinance_table.keys())
        os.makedirs('./data/Usd_quote', exist_ok=True)
        df.to_csv('./data/Usd_quote/usd_quote.csv', index = False)
        return './data/Usd_quote/usd_quote.csv'
    
    def transform_ticker_gas(self):
    
        yfinance_table = self.engine.execute(  """
                                             SELECT "Date", "Open", "Close", "High", "Low", "Volume" From yfinance WHERE "Ticker_name" ='Gas'
                                             WHERE "Date" BETWEEN (NOW() - INTERVAL '1 year')::DATE AND NOW()::DATE
                                             """)
        df = pd.DataFrame(yfinance_table.fetchall(), columns=yfinance_table.keys())
        os.makedirs('./data/Gas_quote', exist_ok=True)
        df.to_csv('./data/Gas_quote/gas_quote.csv', index = False)
        return './data/Gas_quote/gas_quote.csv'
        
if __name__=='__main__':
    ydata = yFinance2()
    path = ydata.extract_ticker_weekly()
    path2 = ydata.transform_ticker_weekly(path)
    print(ydata.load_ticker_weekly(path2))