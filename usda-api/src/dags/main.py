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

class MachineLearning:
    def __init__(self, path):
        #obj_yfinance  = yFinance2()
        self.path = path
        stock_data_path = path #obj_yfinance.get_csv_path()
        df = pd.read_csv(stock_data_path, parse_dates = ['Date'])
        df.Date = pd.to_datetime(df.Date)
        df.sort_values(by='Date', ascending=True, inplace = True)
        self.data = df
        self.look_back = 30
        self.look_next = 1
        self.scalar = StandardScaler()
        self.train_test_split = 0.9
        self.train = self.data.iloc[0: int(len(self.data)*self.train_test_split)]
        self.val = self.data.iloc[int(len(self.data)*self.train_test_split): ]
        self.defualt_weights_path = 'training_1/m.weights.h5'
    
    def reload(self):
        stock_data_path = self.path #obj_yfinance.get_csv_path()
        df = pd.read_csv(stock_data_path, parse_dates = ['Date'])
        df.Date = pd.to_datetime(df.Date)
        df.sort_values(by='Date', ascending=True, inplace=True)
        self.data = df
        return
    
    def analyze_stock_prices(self, num_view):
        num = -num_view
        current_df = self.data[['Date', 'Open', 'Close', 'High', 'Low', 'Volume']].copy(deep =True)
        current_df['Date'] = pd.to_datetime(current_df['Date'], utc=True)
        current_df.set_index('Date', inplace=True)
        os.makedirs('./analyse', exist_ok= True)
        mpf.plot(current_df[num:], type='candle', style='charles', title='Stock Price Analysis with XGBoost Predictions',
                ylabel='Price', ylabel_lower='Volume', volume=True, savefig='./analyse/analyse.png')
        img = Image.open('./analyse/analyse.png')
        return img
    
    
    
    def LSTM_model(self):
        model = Sequential()
        model.add(keras.Input(shape=(self.look_back, 1)))
        model.add(LSTM(
            units=self.look_back,
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
            units=self.look_next,))
        model.add(Activation('linear'))
        #model.compile(loss='mse', optimizer = 'SGD', metrics=['mean_absolute_error'])
        return model
    
    def create_dataset(self, dataset):
        dataset = self.scalar.fit_transform(np.array(dataset['Close'].tolist()).reshape(-1, 1))
        dataX, dataY = [], []
        for i in range(len(dataset)-self.look_back-self.look_next):
            m=dataset[i: i+self.look_back]
            m2 = dataset[i+self.look_back:i+self.look_back+self.look_next]
            dataX.append(m.reshape(1,-1).tolist()[0])
            dataY.append(m2.reshape(1, -1).tolist()[0])
        print(np.array(dataX).shape) 
        dataX, dataY = np.array(dataX), np.array(dataY)
        dataX = np.reshape(dataX, (dataX.shape[0], self.look_back, 1))
        return dataX, dataY
    
    
    def LSTM_train(self, num_epoch, optimizer):
        model = self.LSTM_model()
        checkpoint_path = os.path.join(os.path.curdir,'training_2/a1.best.weights.h5')
        checkpoint_dir = os.path.dirname(checkpoint_path)
        os.makedirs(checkpoint_dir, exist_ok = True)
        checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(filepath= checkpoint_path ,save_best_only= True, save_weights_only= True, monitor = 'val_loss')
        
        trainX, trainY = self.create_dataset(self.train)
        valX, valY = self.create_dataset(self.val)

        model.compile(loss='mse', optimizer = optimizer, metrics = ['mean_absolute_error'])
        
        model.fit(trainX, trainY, batch_size = 16, epochs = num_epoch, validation_split = 0.1, callbacks= [checkpoint_callback])
        
        return checkpoint_path, valX, valY
    
    def get_weights_dir(self):
        return os.path.join(os.path.curdir,'training_2/a1.best.weights.h5')   
    
    def evaluate(self, num_epoch, optimizer):
        
        weights_dir, valX, valY = self.LSTM_train(num_epoch, optimizer)
        model = self.LSTM_model()
        model.load_weights(weights_dir)
        prediction_1 = model.predict(valX)
        actual = self.scalar.inverse_transform(valY[:].reshape(-1, 1))
        prediction_2 = self.scalar.inverse_transform(prediction_1[:].reshape(-1,1))
        
        os.makedirs('./evaluate', exist_ok = True)
        file_path = os.path.join(os.curdir,'evaluate/evaluate.png')
        val_1 = self.val.copy(deep=True)
        val_1['Date'] = pd.to_datetime(val_1['Date'], utc= True)
        plt.plot_date(val_1['Date'].iloc[self.look_back+self.look_next:],actual, color='orange', fmt='-',label = 'Actual')
        plt.gcf().autofmt_xdate()
        date_format = mdates.DateFormatter('%d, %b, %Y')
        plt.gca().xaxis.set_major_locator(MaxNLocator(5))
        plt.gca().xaxis.set_major_formatter(date_format)    
        plt.plot_date(val_1['Date'].iloc[self.look_back+self.look_next:],prediction_2, color = 'lightgreen',fmt='-' ,label = 'Predict')
        plt.legend()
        plt.title('LSTM Evaluation of Closing Price')
        plt.savefig(file_path)
        plt.close()
        return file_path  
    
    def train_model(self, num_epoch, optimizer):
        img_path = self.evaluate(num_epoch, optimizer)
        img = Image.open(img_path)
        return img
     
    def forecast_stock_prices(self, num_day):
        df = self.data[-self.look_back:].copy(deep=True)
        window = self.scalar.fit_transform(np.array(df['Close'].tolist()).reshape(-1,1))
        model = self.LSTM_model()
        try:
            model.load_weights(self.get_weights_dir())
        except:
            model.load_weights(self.defualt_weights_path)

        prediction = []
        for i in range(num_day):
            window_input = np.reshape(window, (1, window.shape[0], 1))
            value = model.predict(window_input)
            value = value.tolist()
            window_temp = np.roll(window,-1)
            window_temp[-1] = np.array(value)
            prediction.append(value)
            window = window_temp

        if num_day > 0:
            output = self.scalar.inverse_transform(np.array(prediction).reshape(-1,1))
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
    
    def gradio(self):
        iface = gr.Interface(fn=self.analyze_stock_prices, 
            inputs = [gr.Slider(minimum=0, maximum=365, step=1, label='View previous x days price:')],   
            outputs=['image'],
            allow_flagging= 'never'
            )
        iface2 = gr.Interface(
            fn = self.train_model,
            inputs = [
                gr.Slider(minimum = 1, maximum = 100, step = 1, label = 'Number of epochs'),
                gr.Radio(['SGD', 'Adam'], label = 'Optimizer', value ='SGD')
            ],
            outputs=['image'],
            allow_flagging= 'never'
        )
        iface3 = gr.Interface(
            fn = self.forecast_stock_prices,
            inputs = [
                gr.Slider(minimum=0, maximum=30, step =1, label='Predict next x days price')]
            ,
            outputs= ['image'],
            allow_flagging = "never"
        )
        

        with gr.Blocks() as demo:
            with gr.Column( elem_classes= ["container"]):
                gr.TabbedInterface([iface, iface2, iface3],
                tab_names=['Price Analysis', 'Training LSTM', 'Forecasting LSTM'])
            
            with gr.Row('Reload'):
                reload_button = gr.Button('Reload')
                
            reload_button.click(
                self.reload,
                [],
                []
            )
                    
        
        demo.launch(share = True)
    
        
if __name__ == '__main__':
    ml = MachineLearning('./Corn_quote/corn_quote.csv')
    ml.gradio()       