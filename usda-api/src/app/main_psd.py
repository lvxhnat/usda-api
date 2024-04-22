import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine

class PSDAnalysis:
    columns_dict = {'Area_Harvested_1000_HA': {'line_label': 'Area_Harvested', 'yaxis': 'Area Harvested (1000 HA)', 'title': 'Forecast of area of corn harvested'},
                    'Yield_MT_per_HA': {'line_label': 'Yield', 'yaxis': 'Corn Yield (MT/HA)', 'title': 'Forecast of yield of Corn'}}

    def __init__(self, conn_str=None, data_path = '../data/extract_psd/extract_psd.csv'):
        if conn_str != None:
            self.data = self.get_psd_data_sql(conn_str)
        else:
            self.data = self.get_psd_data_file(data_path)

    def get_psd_data_sql(self, conn_str):
        engine = create_engine(conn_str)
        psd_cloudsql_metadata = self.engine.execute("SELECT * From psd")
        psd_cloudsql_df = pd.DataFrame(psd_cloudsql_metadata.fetchall(), columns=psd_cloudsql_metadata.keys())
        psd_cloudsql_df['Date'] = pd.to_datetime(psd_cloudsql_df['Date'])
        return psd_cloudsql_df
    
    def get_psd_data_file(self, file_path):
        psd_df = pd.read_csv(file_path)
        psd_df['Date'] = pd.to_datetime(psd_df['Date'])
        return psd_df
    
    def create_plot(self, column):
        line_label = PSDAnalysis.columns_dict[column]['line_label']
        title = PSDAnalysis.columns_dict[column]['title']
        yaxis = PSDAnalysis.columns_dict[column]['yaxis']

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=self.data['Date'], y=self.data[column], mode='lines+markers', name=line_label))
        fig.update_layout(title=title,
                          xaxis_title='Date',
                          yaxis_title=yaxis,
                          showlegend=True)
        return fig


    


if __name__ == '__main__':
    #psd = PSDAnalysis('postgresql://postgres:Password*1@35.239.18.20/jy')
    psd = PSDAnalysis()
    area_harvested = psd.create_plot('Area_Harvested_1000_HA')