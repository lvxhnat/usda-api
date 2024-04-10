import plotly.graph_objects as go
import plotly
from plotly.subplots import make_subplots
import pandas as pd

class Indices:
    def __init__(self): 
        pass
    
    def create_plot(self, period, name, df):
        today = pd.to_datetime('today')
        
        if period == '5 days':
            df = df.iloc[-5:]
        elif period == '1m':
            one_month_ago = today - pd.DateOffset(months= 1)
            df = df[(df.Date <= today) & (df.Date > one_month_ago)]
        else:
            df = df
        df.set_index('Date', inplace= True)
           
        if 'USD' not in name:
            fig = make_subplots(rows = 2, cols =1,
                                shared_xaxes = True,
                                vertical_spacing= 0.01,
                                row_heights=[0.85,0.15])
            fig.add_trace(go.Candlestick(x=df.index,
                                        low = df.Low,
                                        high = df.High,
                                        close = df.Close,
                                        open = df.Open,
                                        increasing_line_color = 'green',
                                        decreasing_line_color = 'red'),
                        row = 1, col = 1)
            
            fig.add_trace(go.Bar(x=df.index, y = df.Volume),
                        col = 1, row = 2)
            fig.update_layout(title = f'Daily {name} Index',
                            yaxis1_title = 'Price ($)',
                            yaxis2_title = 'Volume',
                            xaxis2_title = 'Time',
                            xaxis1_rangeslider_visible = False,
                            xaxis2_rangeslider_visible = False)
            
        else:
            fig = make_subplots(rows = 1, cols =1,
                            shared_xaxes = True,
                            vertical_spacing= 0.01)
            fig.add_trace(go.Candlestick(x=df.index,
                                        low = df.Low,
                                        high = df.High,
                                        close = df.Close,
                                        open = df.Open,
                                        increasing_line_color = 'green',
                                        decreasing_line_color = 'red'),
                        row = 1, col = 1)
            fig.update_layout(title = f'Daily {name} Index',
                            yaxis1_title = 'Price ($)',
                            xaxis1_title = 'Time',
                            xaxis1_rangeslider_visible = False,)
            
        return fig
    
    def analyze_oil_prices(self, period = '1y'):
        df = pd.read_csv('../data/Oil_quote/oil_quote.csv', parse_dates=['Date'])
        df.Date = pd.to_datetime(df.Date, utc = False)
        df.sort_values(by = 'Date', ascending=True, inplace = True)
        return self.create_plot(period, 'Oil (CL=F)', df)
        
    def analyze_usd_prices(self, period = '1y'):
        df = pd.read_csv('../data/Usd_quote/usd_quote.csv', parse_dates=['Date'])
        df.Date = pd.to_datetime(df.Date, utc = False)
        df.sort_values(by = 'Date', ascending=True, inplace = True)
        return self.create_plot(period, 'USD (DX-Y.NYB)', df)
    
    def analyze_gas_prices(self, period = '1y'):
        df = pd.read_csv('../data/Gas_quote/gas_quote.csv', parse_dates=['Date'])
        df.Date = pd.to_datetime(df.Date, utc = False)
        df.sort_values(by = 'Date', ascending=True, inplace = True)
        return self.create_plot(period, 'Gas (NG=F)', df)