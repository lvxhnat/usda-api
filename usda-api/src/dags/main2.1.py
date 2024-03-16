from usda_api.scrapers.yFinance2.yFinance2 import yFinance2
import gradio as gr
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import io
from PIL import Image
from datetime import datetime, timedelta
import pytz

obj_yfinance = yFinance2()

stock_data_csv = obj_yfinance.get_ticker_history_max_period().get_csv_path()
def analyze_stock_prices(num, year, noise):
    obj_yfinance = yFinance2()
    stock_data_csv = obj_yfinance.get_ticker_history_max_period().get_csv_path()
    stock_data = pd.read_csv(stock_data_csv)
    print(stock_data.head())
    print(year)
    num = -num
    if year == None:
        current_df = stock_data[['Date', 'Open', 'Close', 'High', 'Low', 'Volume']]
        current_df['Date'] = pd.to_datetime(current_df['Date'], utc=True)
        current_df.set_index('Date', inplace=True)
        mpf.plot(current_df[num:], type='candle', style='charles', title='Stock Price Analysis with XGBoost Predictions',
                    ylabel='Price', ylabel_lower='Volume', volume=True, savefig='testsave.png')
        img = Image.open('testsave.png')
        return img
    else:
        end_date = datetime.strptime(f'{year}-12-31 00:00:00+00:00', "%Y-%m-%d %H:%M:%S%z")
        current_date = datetime.now(tz=pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        datetime_list = []
        while current_date <= end_date:
            if current_date.weekday() < 5:
                datetime_list.append(current_date)
            current_date += timedelta(days =1)
            current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        total_period = len(datetime_list)
        import numpy as np
        info_list = ['Open', 'Close', 'High', 'Low', 'Volume']
        info_dict= {'Date': datetime_list,
                    'Open': [],
                    'Close': [],
                    'High' : [],
                    'Low': [],
                    'Volume' :[]}
        for i in info_list:
            mean_value = stock_data[i].mean()
            std_value = stock_data[i].std()
            future = np.random.normal(mean_value, std_value+noise, total_period)
            #noise = np.random.normal(0, noise, total_period)
            future_noise = future # + noise
            future_value = [stock_data[i].iloc[-1] * (1 + future_noise[k]) for k in range(total_period)]
            
            info_dict[i] = future_value
        
        future_df = pd.DataFrame(info_dict)
        future_df['Date'] = pd.to_datetime(future_df['Date'], utc=True)
        future_df.set_index('Date', inplace=True)
        mpf.plot(future_df[num:], type='candle', style='charles', title='Stock Price Analysis with XGBoost Predictions',
                ylabel='Price', ylabel_lower='Volume', volume=True, savefig='testsave.png')
        img = Image.open('testsave.png')
        return img
    

iface = gr.Interface(fn=analyze_stock_prices, 
    inputs = [gr.Slider(minimum=0, maximum=365, step=1, label='Previous x days price:'),
            gr.Radio([None, 2025,2030], label='Project to:'),
            gr.Slider(0,2, label='Noise Level')],   
    outputs='image',
    )
iface.launch()
