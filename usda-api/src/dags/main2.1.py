from usda_api.scrapers.yFinance2.yFinance2 import yFinance2
import gradio as gr
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
from PIL import Image
from datetime import datetime, timedelta
import pytz

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import tensorflow as tf
from tensorflow import keras
from subprocess import check_output
from keras.layers import Dense, Activation, Dropout
from keras.layers import LSTM
from keras.models import Sequential
import time #helper libraries
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import os

import matplotlib.dates as mdates
from datetime import datetime
from matplotlib.ticker import MaxNLocator

obj_yfinance = yFinance2()
stock_data_csv = obj_yfinance.get_ticker_history_max_period().get_csv_path()
def analyze_stock_prices(num, year, noise):
    obj_yfinance = yFinance2()
    stock_data_csv = obj_yfinance.get_ticker_history_max_period().get_csv_path()
    stock_data = pd.read_csv(stock_data_csv)
    print(stock_data.head())
    print(year)
    num = -num
    #if year == None:
    current_df = stock_data[['Date', 'Open', 'Close', 'High', 'Low', 'Volume']]
    current_df['Date'] = pd.to_datetime(current_df['Date'], utc=True)
    current_df.set_index('Date', inplace=True)
    os.makedirs('./analyse', exist_ok= True )
    mpf.plot(current_df[num:], type='candle', style='charles', title='Stock Price Analysis with XGBoost Predictions',
                ylabel='Price', ylabel_lower='Volume', volume=True, savefig='./analyse/analyse.png')
    img = Image.open('./analyse/analyse.png')
    return img
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
    os.makedirs('./analyse')
    mpf.plot(future_df[num:], type='candle', style='charles', title='Stock Price Analysis with XGBoost Predictions',
            ylabel='Price', ylabel_lower='Volume', volume=True, savefig='./analyse/testsave.png')
    img = Image.open('./analyse/testsave.png')
    return img

def LSTM_model(look_back = 30, look_next = 1):
    model = Sequential()
    model.add(keras.Input(shape=(look_back, 1)))
    model.add(LSTM(
        units=look_back,
        return_sequences=True))
    model.add(Dropout(0.2))

    model.add(LSTM(
        100,
        return_sequences=True))
    model.add(Dropout(0.2))

    model.add(LSTM(
        100,
        return_sequences=True))
    model.add(Dropout(0.2))

    model.add(LSTM(
        100,
        return_sequences=False))
    model.add(Dropout(0.2))

    model.add(Dense(
        units=look_next,))
    model.add(Activation('linear'))
    #model.compile(loss='mse', optimizer = 'SGD', metrics=['mean_absolute_error'])
    return model
def create_dataset(dataset, look_back, look_next):
    dataX, dataY = [], []
    for i in range(len(dataset)-look_back-look_next):
        m=dataset[i: i+look_back]
        m2 = dataset[i+look_back:i+look_back+look_next]
        dataX.append(m.reshape(1,-1).tolist()[0])
        dataY.append(m2.reshape(1, -1).tolist()[0])
    return np.array(dataX), np.array(dataY)

def LSTM_train(train_1, val_1,num_epoch, optimizer, look_back = 30, look_next=1):
    model = LSTM_model()
    checkpoint_path = os.path.join(os.path.curdir,'training_2/a1.best.weights.h5')
    checkpoint_dir = os.path.dirname(checkpoint_path)
    os.makedirs(checkpoint_dir, exist_ok = True)
    checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(filepath= checkpoint_path ,save_best_only= True, save_weights_only= True, monitor = 'val_loss')
    scalar = StandardScaler()
    train_2 = scalar.fit_transform(np.array(train_1['Close'].tolist()).reshape(len(train_1), 1))
    val_2 = scalar.fit_transform(np.array(val_1['Close'].tolist()).reshape(len(val_1), 1))
    trainX_1, trainY_1 = create_dataset(train_2, look_back, look_next)
    valX_1, valY_1 = create_dataset(val_2, look_back, look_next)
    
    trainX_2 = np.reshape(trainX_1, (trainX_1.shape[0], look_back, 1))
    valX_2 = np.reshape(valX_1, (valX_1.shape[0], look_back, 1))
    
    model.compile(loss='mse', optimizer = optimizer, metrics = ['mean_absolute_error'])
    
    model.fit(trainX_2, trainY_1, batch_size = 16, epochs = num_epoch, validation_split = 0.1, callbacks= [checkpoint_callback])
    
    return checkpoint_path, valX_2, valY_1, scalar
def get_weights_dir():
    return os.path.join(os.path.curdir,'training_2/a1.best.weights.h5')
def get_data_frame(data_path='history.csv'):
    df = pd.read_csv(data_path)
    return df
def evaluate(num_epoch, optimizer, data_path='history.csv', look_back = 30, look_next=1, train_test_split = 0.8):
    df = get_data_frame(data_path)
    df_size = len(df)
    train_1 = df.iloc[0:int(df_size * train_test_split)]
    val_1 = df.iloc[int(df_size*train_test_split):]
    weights_dir, valX, valY, scalar = LSTM_train(train_1, val_1,num_epoch, optimizer, look_back, look_next)
    model = LSTM_model()
    model.load_weights(weights_dir)
    prediction_1 = model.predict(valX)
    actual = scalar.inverse_transform(valY[:].reshape(-1, 1))
    prediction_2 = scalar.inverse_transform(prediction_1[:].reshape(-1,1))
    
    os.makedirs('./evaluate', exist_ok = True)
    file_path = os.path.join(os.curdir,'evaluate/evaluate.png')
    val_1['Date'] = pd.to_datetime(val_1['Date'])
    plt.plot_date(val_1['Date'].iloc[look_back+look_next:],actual, color='orange', fmt='-',label = 'Actual')
    plt.gcf().autofmt_xdate()
    date_format = mdates.DateFormatter('%d, %b, %Y')
    plt.gca().xaxis.set_major_locator(MaxNLocator(5))
    plt.gca().xaxis.set_major_formatter(date_format)    
    plt.plot_date(val_1['Date'].iloc[look_back+look_next:],prediction_2, color = 'lightgreen',fmt='-' ,label = 'Predict')
    plt.legend()
    plt.title('LSTM Evaluation of Closing Price')
    plt.savefig(file_path)
    plt.close()
    return file_path
    
def train_model(num_epoch=1, optimizer = 'SGD'):
       img_path = evaluate(num_epoch, optimizer)
       img = Image.open(img_path)
       return img
    
def forecast_stock_prices(n_day = 0, look_back = 30):

    df = get_data_frame()
    df = df.iloc[-look_back:]
    scalar = StandardScaler()
    window = scalar.fit_transform(np.array((df['Close']).tolist()).reshape(-1, 1))
    model = LSTM_model()
    try:
        model.load_weights(get_weights_dir())
    except:
        model.load_weights('./training_1/m.weights.h5')

    prediction = []

    for i in range(n_day):
        window_input = np.reshape(window, (1, window.shape[0],1))
        value = model.predict(window_input)
        value = value.tolist()
        window_temp = np.roll(window, -1)
        window_temp[-1] = np.array(value)
        prediction.append(value)
        window = window_temp
    if n_day >0:
        output = scalar.inverse_transform(np.array(prediction).reshape(-1,1))
    else:
        output = []
    plt.plot(output, color = 'lightblue', marker ='o')
    os.makedirs('./predict', exist_ok=True)
    file_path= os.path.join(os.path.curdir,'predict/predict.png')
    plt.xlabel('days')
    plt.title('LSTM Forecasting of Closing Price')
    plt.savefig(file_path)
    plt.close()
    
    img = Image.open(file_path)
    
    return img
