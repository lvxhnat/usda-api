from sqlalchemy import create_engine
from datetime import date, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd


class Ethanol:
    def __init__(self):
        data_frame = pd.read_csv('../data/Ethanol_quote/eia_quote.csv', parse_dates=['Date'])
        data_frame.Date = pd.to_datetime(data_frame.Date)
        self.df = data_frame

    def create_ethanol_plot(self):
        fig = px.line(self.df, x='Date', y='Value', title='Ethanol Production Over Time (Thousand Barrels / Day)')

        # Update layout to add range slider
        fig.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1M", step="month", stepmode="backward"),
                        dict(count=6, label="6M", step="month", stepmode="backward"),
                        dict(count=1, label="1Y", step="year", stepmode="backward"),
                        dict(step="all")
                    ])
                ),
                rangeslider=dict(
                    visible=True
                ),
                type="date"
            )
        )

        return fig