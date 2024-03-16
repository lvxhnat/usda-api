from datetime import datetime
import pandas as pd
import numpy as np
import json
import yfinance as yf

class yFinance:
    def __init__(self):
        self.ticker = 'ZC=F'
        self.data =["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "Tickers", "Market Status"] 
        self.common = None
        
    def get_ticker_obj(self):
        return yf.Ticker(self.ticker)
    
    def get_ticker(self):
        info =  self.get_ticker_obj()
        return info
    def get_ticker_history_max_period(self):
        info = self.get_ticker_obj()
        if self.common is None:
            info.history(period="max").to_csv('history.csv')
            pd2 = pd.read_csv('history.csv')
            self.common = pd2
            pd2 = pd2[['Date', 'Open', 'High', 'Low','Close', 'Volume']]
            #pd2.to_csv('history.csv')
            return self
        else:
            'common exist'
        return self
    def get_ticker_history_period(self):
        info = self.get_ticker_obj()
        return info.history(period="1wk")
    def get_csv_path(self):
        return 'history.csv'