from datetime import datetime
import pandas as pd
import numpy as np
import json
import yfinance as yf
TICKER = ['ZC=F', 'CL=F', 'NG=F', 'DX-Y.NYB']
class yFinance2:
    def __init__(self, ticker = 'ZC=F'):
        self.ticker = ticker
        self.data =["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "Tickers", "Market Status"] 
        self.common = None
        self.info = None
        self.data_frame = None
        
    def extract_ticker(self):
        self.info = yf.Ticker(self.ticker)
        return self
    
    '''def transform_and_load_ticker_max_period(self, info):
        if self.common is None:
            info.history(period="max").to_csv(f'{self.ticker}_history.csv')
            pd2 = pd.read_csv('history.csv')
            self.common = pd2
            pd2 = pd2[['Date', 'Open', 'High', 'Low','Close', 'Volume']]
            #pd2.to_csv('history.csv')
            return self
        else:
            print('common exist')
        return self
    
    def transform_and_load_ticker_period(self, info):
        df = pd.DataFrame(info.history(period="1wk"))
        print(df.head(5))'''
    
    def transform_ticker(self):
        if self.info is not None:
            self.data_frame = pd.DataFrame(self.info.history(period="max"))
        else:
            print('Please extract data')
        return self
            
    def load_ticker(self):
        if self.data_frame is not None:
            self.data_frame.to_csv(f'{self.ticker}_history.csv')
            print('success')
        else:
            print('Please transform data')
        return self
    
    def get_csv_path(self):
        return f'{self.ticker}_history.csv'
            
        
if __name__=='__main__':
    ydata = yFinance2('DX-Y.NYB')
    ydata.extract_ticker()
    ydata.transform_ticker()
    ydata.load_ticker()