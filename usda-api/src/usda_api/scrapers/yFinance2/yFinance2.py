from datetime import datetime
import pandas as pd
import numpy as np
import json
import yfinance as yf
import os
'''TICKER = ['ZC=F', 'CL=F', 'NG=F', 'DX-Y.NYB']
class yFinance2:
    def __init__(self, ticker = 'ZC=F'):
        self.ticker = ticker
        self.data =["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "Tickers"] 
        
    def extract_ticker(self):
        info = yf.Ticker(self.ticker)
        dir = f'../dataset_extract'
        path = f'../dataset_extract/{self.ticker}.csv'
        os.makedirs(dir, exist_ok = True)
        info.history(period='max').to_csv(path)
        return path
    
    def transform_and_load_ticker_max_period(self, info):
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
        print(df.head(5))
    
    def transform_ticker(self, path):
        dir = f'../dataset_transform'
        path = f'../dataset_transform/{self.ticker}.csv'
        os.makedirs(dir, exist_ok = True)        
        data_frame = pd.read_csv(path)
        df = data_frame[self.data]
        df.to_csv(path)
        return path
        
            
    def load_ticker(self, path):
        print('success csv file created ')
        return

    
    def get_csv_path(self):
        return f'{self.ticker}_history.csv'
            
        
if __name__=='__main__':
    ydata = yFinance2('DX-Y.NYB') 
'''
    
    
from datetime import datetime
import pandas as pd
import numpy as np
import json
import yfinance as yf
import os
TICKER = ['ZC=F', 'CL=F', 'NG=F', 'DX-Y.NYB']
class yFinance2:
    def __init__(self):
        self.data =["Date", "Open", "High", "Low", "Close", "Volume"] 
        self.TICKER = TICKER
    def extract_ticker(self):
        dir = f'./extract_yfinance'
        os.makedirs(dir, exist_ok = True)
        for t in self.TICKER:
            path = f'./extract_yfinance/{t}.csv'
            info = yf.Ticker(t)
            info.history(period='max').to_csv(path)
        return dir
    
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
    
    def transform_ticker(self, path):
        dir = f'./transform_yfinance'
        os.makedirs(dir, exist_ok = True)        
        for t in self.TICKER:
            path_temp = path + '/' + t + '.csv'
            data_frame = pd.read_csv(path_temp)
            df = data_frame[self.data]
            path2 = dir + '/' + t + '.csv'
            df.to_csv(path2)
        return dir
        
            
    def load_ticker(self, path):
        print('success csv file created ')
        return

    
    def get_csv_path(self):
        return f'{self.ticker}_history.csv'
            
        
if __name__=='__main__':
    ydata = yFinance2('DX-Y.NYB')