import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine
from plotly.subplots import make_subplots

class PSDAnalysis:
    def __init__(self, conn_str=None, data_path = '../data/extract_psd/extract_psd.csv'):
        if conn_str != None:
            self.data = self.get_psd_data_sql(conn_str)
        else:
            self.data = self.get_psd_data_file(data_path)

    def get_psd_data_sql(self, conn_str):
        engine = create_engine(conn_str)
        psd_cloudsql_metadata = engine.execute("SELECT * From psd")
        psd_cloudsql_df = pd.DataFrame(psd_cloudsql_metadata.fetchall(), columns=psd_cloudsql_metadata.keys())
        psd_cloudsql_df['Date'] = pd.to_datetime(psd_cloudsql_df['Date'])
        return psd_cloudsql_df
    
    def get_psd_data_file(self, file_path):
        psd_df = pd.read_csv(file_path)
        psd_df['Date'] = pd.to_datetime(psd_df['Date'])
        return psd_df
    
    def get_psd_data(self):
        return self.data
    
    def general_plot(self, column, line_label, title, yaxis, color):
        data = self.get_psd_data() # to change
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=data['Date'], y=data[column], mode='lines+markers', name=line_label, line=dict(color=color)))
        fig.update_layout(title=title,
                            xaxis_title='Date',
                            yaxis_title=yaxis,
                            showlegend=True)
        return fig

    def dual_plot(self, column1, column2, line_label1, line_label2, title, y_axis1, y_axis2, color):
        data = self.get_psd_data()
        # Create a subplot
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        # Add traces on the primary y-axis
        fig.add_trace(go.Scatter(x=data['Date'], y=data[column1], mode='lines+markers', name=line_label1), secondary_y=False)
        # Add traces on the secondary y-axis
        fig.add_trace(go.Scatter(x=data['Date'], y=data[column2], mode='lines+markers', name=line_label2, line=dict(color=color)), secondary_y=True)
        # Update layout
        fig.update_layout(title=title,
                        xaxis_title="Date",
                        yaxis_title=y_axis1,
                        yaxis2_title=y_axis2)
        return fig

    def single_dual_plot(self, column1, column2, line_label1, line_label2, title, yaxis, color):
        data = self.get_psd_data()
        fig = go.Figure()
        # Add trace for the first line
        fig.add_trace(go.Scatter(x=data['Date'], y=data[column1], mode='lines+markers', name=line_label1))
        # Add trace for the second line
        fig.add_trace(go.Scatter(x=data['Date'], y=data[column2], mode='lines+markers', name=line_label2, line=dict(color=color)))
        # Update layout
        fig.update_layout(title=title,
                        xaxis_title="Date",
                        yaxis_title=yaxis)
        return fig
    
    def area_harvested_plot(self):
        plot = self.general_plot(column = 'Area_Harvested_1000_HA',
                                 line_label = 'Area_Harvested',
                                 title = 'Forecast of area of Corn harvested in US',
                                 yaxis = 'Area Harvested (1000 HA)',
                                 color = 'red')
        return plot
    
    def yield_plot(self):
        plot = self.general_plot(column = 'Yield_MT_per_HA',
                                 line_label = 'Yield (MT)',
                                 title = 'Forecast of yield of Corn in US',
                                 yaxis = 'Corn Yield (MT/HA)',
                                 color = 'blue')
        return plot

    def total_distribution_plot(self):
        plot = self.general_plot(column = 'Total_Distribution_1000_MT',
                                 line_label = 'Total Distribution (1000 MT)',
                                 title = 'Forecast of Total Distribution of Corn in US',
                                 yaxis = 'Total Distribution (1000 MT)',
                                 color = 'green')
        return plot
    
    def production_vs_domestic_consumption_plot(self):
        plot = self.dual_plot(column1 = 'Production_1000_MT',
                              column2 = 'Domestic_Consumption_1000_MT',
                              line_label1 = "Production (1000 MT)",
                              line_label2 = "Domestic Consumption (1000 MT)",
                              title = "Production vs. Domestic Consumption",
                              y_axis1 = "Production (1000 MT)",
                              y_axis2 = "Domestic Consumption (1000 MT)",
                              color='red')
        return plot

    def production_vs_fsi_consumption_plot(self):
        plot = self.dual_plot(column1 = 'Production_1000_MT',
                              column2 = 'FSI_Consumption_1000_MT',
                              line_label1 = "Production (1000 MT)",
                              line_label2 = "FSI Consumption (1000 MT)",
                              title = "Production vs. FSI Consumption",
                              y_axis1 = "Production (1000 MT)",
                              y_axis2 = "FSI Consumption (1000 MT)",
                              color = 'green')
        return plot
    
    def production_vs_feed_dom_consumption_plot(self):
        plot = self.dual_plot(column1 = 'Production_1000_MT',
                              column2 = 'Feed_Dom_Consumption_1000_MT',
                              line_label1 = "Production (1000 MT)",
                              line_label2 = "Feed Domestic Consumption (1000 MT)",
                              title = "Production vs. Feed Domestic Consumption",
                              y_axis1 = "Production (1000 MT)",
                              y_axis2 = "Feed Dom Consumption (1000 MT)",
                              color = 'black')
        return plot
    
    def inventory_plot(self):
        plot = self.single_dual_plot(column1 = 'Ending_Stocks_1000_MT',
                                     column2 = 'Beginning_Stocks_1000_MT',
                                     line_label1 = 'Ending Stocks',
                                     line_label2 = 'Beginning Stocks',
                                     title = 'Beginning vs Ending Stocks',
                                     yaxis = 'Stocks (1000 MT)',
                                     color = 'red')
        return plot
    
    def export_plot(self):
        plot = self.general_plot(column = 'TY_Exports_1000_MT',
                                 line_label = 'Exports (1000_MT)',
                                 title = 'Forecast of Exports',
                                 yaxis = 'Exports (1000 MT)',
                                 color = 'blue')
        return plot
    
    def export_import_plot(self):
        plot = self.dual_plot(column1 = 'TY_Exports_1000_MT',
                              column2 = 'TY_Imports_1000_MT',
                              line_label1 = 'TY Exports (1000 MT)',
                              line_label2 = 'TY Imports (1000 MT)',
                              title = 'Forecast of Exports vs Imports',
                              y_axis1 = 'TY Exports (1000 MT)',
                              y_axis2 = 'TY Imports (1000 MT)',
                              color = 'red')
        return plot


    


if __name__ == '__main__':
    psd = PSDAnalysis()
    area_harvested = psd.create_plot('Area_Harvested_1000_HA')