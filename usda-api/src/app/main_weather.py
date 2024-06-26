from sqlalchemy import create_engine
from datetime import date, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
class WeatherAnalysis:
    def __init__(self):
        self.states = ["Iowa", "Illinois","South Dakota" ,"Nebraska", "Minnesota", "Indiana"]
        df = pd.read_csv('../data/Weather_quote/weather_quote.csv', parse_dates= ['Date'])
        df.Date = pd.to_datetime(df.Date)
        self.df = df

    def create_precipitation_plot(self):
        df = self.df
        states = self.states
        code = {
        'Iowa' : 'IA',
        'Indiana' : 'IN',
        'Illinois' : 'IL',
        'Nebraska' : 'NE',
        'South Dakota' : 'SD',
        'Minnesota' : 'MN'
        }
        df['Year'] = df.Date.dt.year
        df['Month'] = df.Date.dt.month
        df_precipitation_state = df.groupby(["State", 'Year', 'Month']).mean('Precipitation_sum').reset_index()
        df_precipitation_state['Code'] = df_precipitation_state['State'].map(code)
        df_precipitation_state['Time'] = df_precipitation_state['Month'].astype(str) + '-' +df_precipitation_state['Year'].astype(str)
        fig = px.choropleth(df_precipitation_state,
                            color= 'Precipitation_sum',
                            locations= 'Code',
                            color_continuous_scale = 'blues',
                            locationmode= 'USA-states',
                            scope= 'usa',
                            animation_frame= 'Time',
                            labels= {'Precipitation_sum' : 'Avg Precipitation (mm)'},
                            hover_name= 'State',
                            hover_data='Precipitation_sum',
                            
                            )
        fig.add_scattergeo(
            locations=df_precipitation_state['Code'],
            locationmode='USA-states',
            text=df_precipitation_state['Code'],
            textfont=dict(color='red', size = 8),
            mode='text')

        fig.update_layout(
            height= 600,
            title={'text':'Analyzing Average Precipitation Sum Over Time',
                'xanchor':'center',
                'yanchor':'top',
                'x':0.5},)
        return fig
    

    def create_temperature_plot(self):
        df = self.df
        states = self.states
        code = {
            'Iowa': 'IA',
            'Indiana': 'IN',
            'Illinois': 'IL',
            'Nebraska': 'NE',  # Corrected from 'Nebraskda' to 'Nebraska'
            'South Dakota': 'SD',
            'Minnesota': 'MN'
        }

        fig3 = make_subplots(rows=len(states), cols=1, subplot_titles=states)
        color_map = {
            'Temperature_2m_min': 'blue',
            'Temperature_2m_max': 'red',
            'Harvest Period': 'green'  # Color for the harvest period
        }
        show_legend = True

        for i, state in enumerate(states, start=1):
            df_temp = df[df['State'] == state]
            df_temp.dropna(inplace=True)

            # Determine harvest period (September to November)
            df_temp['Month'] = df_temp['Date'].dt.month
            harvest_df = df_temp[df_temp['Month'].isin([9, 10, 11])]

            df_long = df_temp.melt(id_vars=['Date'], value_vars=['Temperature_2m_min', 'Temperature_2m_max'],
                                var_name='Temperature_Type', value_name='Temperature')

            for temp_type in df_long['Temperature_Type'].unique():
                df_type = df_long[df_long['Temperature_Type'] == temp_type]
                fig3.add_trace(go.Scatter(x=df_type.Date, y=df_type.Temperature, mode='lines', name=temp_type,
                                        line=dict(color=color_map[temp_type]), showlegend=show_legend),
                            row=i, col=1)

            # Add harvest period data
            if not harvest_df.empty:
                fig3.add_trace(go.Scatter(x=harvest_df.Date, y=harvest_df['Temperature_2m_min'], mode='lines', name='Harvest Period',
                                        line=dict(color=color_map['Harvest Period']), showlegend=show_legend),
                            row=i, col=1)
                fig3.add_trace(go.Scatter(x=harvest_df.Date, y=harvest_df['Temperature_2m_max'], mode='lines', name='Harvest Period',
                                        line=dict(color=color_map['Harvest Period']), showlegend=False),  # Hide legend for the second trace
                            row=i, col=1)

            show_legend = False  # To avoid repeating legend entries

        fig3.update_layout(height=300 * len(states), title_text="Analysis of Min and Max Temperature by State During Harvest Period")
        fig3.update_yaxes(title_text="Temperature (°C)")
        return fig3





if __name__ == '__main__':
    WeatherAnalysis('./Weather_quote/weather_quote.csv')