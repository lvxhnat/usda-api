import requests
import ast
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime 

class EsrAnalysis:
    def __init__(self):
        data_frame = pd.read_csv('../data/Esr_quote/esr_quote.csv', parse_dates= ['Date'])
        data_frame.Date = pd.to_datetime(data_frame.Date)
        self.df = data_frame
    def create_export_country_plot(self):
        PARAMS = {"API_KEY": "f5458abd-198d-402b-b75e-3ce48527b0d2"}
        url = f"https://apps.fas.usda.gov/OpenData"
        countries_endpoint = "/api/esr/countries"
        countries_response = requests.get(
            url=url + countries_endpoint, headers=PARAMS).text
        df = pd.DataFrame.from_dict(ast.literal_eval(countries_response.replace("null", '"null"')))
        df['countryDescription'] = df['countryDescription'].str.rstrip()
        map = df.set_index("countryDescription")[['gencCode']].to_dict()['gencCode']
        df = self.df
        df['Year'] = df.Date.dt.year
        df['Month'] = df.Date.dt.month
        df_m = df.groupby(['Year', 'Month', 'Country']).sum('grossNewSales').reset_index()
        df_m['code'] = df_m['Country'].map(map)
        df_m['Time'] = df_m['Month'].astype(str) + '-' + df_m['Year'].astype(str)

        fig = px.choropleth(df_m, 
                            locations="code",
                            color= 'grossNewSales', # lifeExp is a column of gapminder
                            hover_name="Country", # column to add to hover information
                            color_continuous_scale=px.colors.sequential.Reds,
                            labels = { 'grossNewSales' :  'grossNewSales (Metrics Ton)'},
                            animation_frame= 'Time',)
        fig.update_layout(
                    height= 800,
                    title={'text':'Corn Sales from the USA Over Time',
                        'xanchor':'center',
                        'yanchor':'top',
                        'x':0.5},)
        return fig
    
    def create_top_10_country_plot(self):
        df = self.df
        
        previous_year = (datetime.datetime.today() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        df_year = df[df['Date'] >= previous_year]
        df_year = df_year[df_year['Country'] != "UNKNOWN"]
        df_year = df_year[['Country', 'grossNewSales']]
        country_sales = df_year.groupby('Country')['grossNewSales'].sum().reset_index()
        top_countries = country_sales.nlargest(10, 'grossNewSales')
        fig_year = px.bar(top_countries, x='Country', y='grossNewSales', color='Country',
                    title='Top 10 Countries by Sales over the Past Year')
        fig_year.update_layout(xaxis_title='Country', yaxis_title='Total Sales (Metric Tons)')
        
        return fig_year
    
    def create_export_weekly_plot(self):
        df = self.df
        df_week = df.groupby(['Date']).sum('grossNewSales').reset_index()
        fig = px.line(df_week, x='Date', y = 'grossNewSales', labels={'grossNewSales' : 'Gross New Sales (Metric Tons)'})
        fig.update_layout(
                    height= 600,
                    title={'text':'Total Weekly Gross New Sales of Corn from the USA',
                        'xanchor':'center',
                        'yanchor':'top',
                        'x':0.5},)
        return fig
    
    