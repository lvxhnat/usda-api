from usda_api.scrapers.yFinance2.yFinance2 import yFinance2
import pandas as pd
from datetime import datetime, timedelta
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


import plotly.graph_objects as go
import plotly
from plotly.subplots import make_subplots
from dash import Dash, Input, Output


class MachineLearning:
    def __init__(self):
        #obj_yfinance  = yFinance2()
        self.path = '../data/Corn_quote/corn_quote.csv'
        stock_data_path = '../data/Corn_quote/corn_quote.csv'#obj_yfinance.get_csv_path()
        df = pd.read_csv(stock_data_path, parse_dates = ['Date'])
        df.Date = pd.to_datetime(df.Date, utc = False)
        df.sort_values(by='Date', ascending=True, inplace = True)
        self.data = df
        self.look_back = 30
        self.look_next = 1
        self.scalar = StandardScaler()
        self.train_test_split = 0.9
        self.train = self.data.iloc[0: int(len(self.data)*self.train_test_split)]
        self.val = self.data.iloc[int(len(self.data)*self.train_test_split): ]
        self.defualt_weights_path = 'training_1/m.weights.h5'

        
    # def reload(self):
    #     stock_data_path = self.path #obj_yfinance.get_csv_path()
    #     df = pd.read_csv(stock_data_path, parse_dates = ['Date'])
    #     df.Date = pd.to_datetime(df.Date)
    #     df.sort_values(by='Date', ascending=True, inplace=True)
    #     self.data = df
    #     return
    
    def analyze_stock_prices(self, period = 'All'):
        
        current_df = self.data[['Date', 'Open', 'Close', 'High', 'Low', 'Volume']].copy(deep =True)
        #current_df['Date'] = pd.to_datetime(current_df['Date'], utc=False)
        
        today = pd.to_datetime('today')
        
        if period == '5 days':
            
            current_df = current_df[-5:]
    
        elif period == '5y':
            five_years_ago = today - pd.DateOffset(years=5)
            current_df = current_df[(current_df['Date'] <= today) & (current_df['Date'] > five_years_ago)]
        
        elif period == '1y':
            one_year_ago = today - pd.DateOffset(years= 1)
            current_df = current_df[(current_df['Date'] <= today) & (current_df['Date'] >  one_year_ago)]
        elif period == '6m':
            six_month_ago = today - pd.DateOffset(months= 6)
            current_df = current_df[(current_df['Date'] <= today) & (current_df['Date'] >  six_month_ago)]
        elif period == '1m':
            one_month_ago = today - pd.DateOffset(months= 1)
            current_df = current_df[(current_df['Date'] <= today) & (current_df['Date'] >  one_month_ago)]
        
        else:
            current_df = current_df
        
        current_df.set_index('Date', inplace=True)
        
        fig = make_subplots(rows = 2, cols =1,
                            shared_xaxes=True,
                            vertical_spacing = 0.01,
                            row_heights=[0.85,0.15])
        
        fig.add_trace(go.Candlestick(x=current_df.index,
                                     low = current_df['Low'],
                                     high = current_df['High'],
                                     close = current_df['Close'],
                                     open = current_df['Open'],
                                     increasing_line_color = 'green',
                                     decreasing_line_color = 'red'),
                      
                      row = 1, col = 1)
        fig.add_trace(go.Bar(x=current_df.index, y = current_df['Volume']),
                      row = 2, col = 1)
        
        fig.update_layout(title = 'Daily Corn Index',
                          yaxis1_title = 'Stock Price ($)',
                          yaxis2_title = 'Volume',
                          xaxis2_title = 'Time',
                          xaxis1_rangeslider_visible  = False,
                          xaxis2_rangeslider_visible = False)
        return fig
    
    
    
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

        if num_epoch == 0:
            return self.defualt_weights_path, valX, valY
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
        
        val_1 = self.val.copy(deep=True)
        val_1['Date'] = pd.to_datetime(val_1['Date'], utc=False)
        
        fig = make_subplots()
        fig.add_trace(go.Scatter(x=val_1['Date'].iloc[self.look_back+self.look_next:], y=actual.flatten(),
                         mode='lines', name='Actual', line=dict(color='orange')))

        # Prediction data
        fig.add_trace(go.Scatter(x=val_1['Date'].iloc[self.look_back+self.look_next:], y=prediction_2.flatten(),
                                mode='lines', name='Predict', line=dict(color='lightgreen')))

        # Update layout with titles and axis labels
        fig.update_layout(title='LSTM Evaluation of Closing Price',
                        xaxis_title='Date',
                        yaxis_title='Price',
                        xaxis=dict(
                            tickmode='auto',
                            nticks=5,  # Adjust the number of ticks as needed
                            tickformat='%d, %b, %Y'  # Date format
                        ))
        return  fig
    
    def train_model(self, num_epoch, optimizer):
        img = self.evaluate(num_epoch, optimizer)
        return img
     
    def forecast_stock_prices(self, num_day, weights):
        df = self.data[-self.look_back:].copy(deep=True)
        window = self.scalar.fit_transform(np.array(df['Close'].tolist()).reshape(-1,1))
        model = self.LSTM_model()
        try:
            if weights:
                model.load_weights(self.defualt_weights_path)

            else:
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
            output_temp = self.scalar.inverse_transform(np.array(prediction).reshape(-1,1))
            output = [df.Close.tolist()[-1]] + output_temp.flatten().tolist()
            
        else:
            output = [df.Close.tolist()[-1]]
            
        fig = make_subplots()
        fig.add_trace(go.Scatter(y=output,  
                         mode='lines+markers', 
                         name='Prediction',
                         line=dict(color='lightblue'),  
                         marker=dict(color='blue')))  

        # Update layout with titles and axis labels
        fig.update_layout(title='LSTM Forecasting of Closing Price',
                        xaxis_title='Days',
                        yaxis_title='Price',
                        template="plotly_white")
        
        return fig
    

        
    
    
        
if __name__ == '__main__':
    ml = MachineLearning('./Corn_quote/corn_quote.csv')