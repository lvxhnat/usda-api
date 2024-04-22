from main_ml import MachineLearning
from main_weather import WeatherAnalysis
from main_ethanol import Ethanol
from main_esr import EsrAnalysis
from main_indices import Indices
from main_psd import PSDAnalysis
from dash import Dash, Input, Output,State, dcc, no_update, html
import dash_bootstrap_components as dbc
class App:
    def __init__(self):
        self.app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        SIDEBAR_STYLE ={      
                "position": "fixed",
                "top": 0,
                "left": 0,
                "bottom": 0,
                "width": "10rem",
                "padding": "2rem 1rem",
                "background-color":"rgba(211, 211, 211, 0.5)",
            }
        CONTENT_STYLE = {
                "margin-left": "10rem",
                "margin-right": "2rem",
                "padding": "2rem 1rem",
            }
        self.sidebar = html.Div(
            [
                html.H4("Dashboard", style={'font-size': '20px'}),
                html.Hr(),
                html.P(
                    "Overview",  style={'font-size': '15px'}
                ),
                dbc.Nav(
                    [
                        dbc.NavLink("Home", href="/", active="exact"),
                        dbc.NavLink("Corn Sales", href="/Corn_Sales", active="exact"),
                        dbc.NavLink("Forecast", href="/Forecast", active="exact"),
                        dbc.NavLink("Weather", href="/Weather", active="exact"),
                        dbc.NavLink("Ethanol", href="/Ethanol", active="exact"),
                        dbc.NavLink("Relevent Indices", href="/Indices", active="exact"),
                        dbc.NavLink("XGBoost Analysis", href="/XGBoost_analysis", active="exact"),
                        dbc.NavLink("LSTM Analysis", href="/LSTM_analysis", active="exact"),
                    ],
                    vertical=True,
                    pills=True,
                ),
            ],
            style= SIDEBAR_STYLE,
        )
        
        self.content = html.Div(id="page-content", children=[], style=CONTENT_STYLE)
        
        self.setup_app_layout()
        self.setup_callbacks()
        
        
    def setup_app_layout(self):
        self.app.layout = html.Div([
            dcc.Location(id='url'),
            self.sidebar,
            self.content
        ])
        
        
    def setup_callbacks(self):
        @self.app.callback(
            Output("page-content", "children"),
            [Input("url", "pathname")]
        )
        
        def render_page_content(pathname):
            if pathname == "/":
                return [
                    html.Div([
                    html.Div([
                        html.H1('Corn Analysis', style={'color': 'white', 'textAlign': 'center'}),
                        html.Hr(),
                        html.P("This dashboard provides an in-depth analysis of corn market trends, including historical price movements, production forecasts, and weather impact assessments.", style={'color': 'white', 'textAlign': 'center', 'padding': '20px'}),
                        html.P("Explore interactive charts and insights to make informed decisions.", style={'color': 'white', 'textAlign': 'center', 'padding': '20px'}),
                    ], style={
                        'backgroundColor': 'rgba(0, 0, 0, 0.5)',  # Semi-transparent black overlay
                        'position': 'absolute',
                        'top': 0,
                        'left': 0,
                        'width': '100%',
                        'height': '100%',
                        'display': 'flex',
                        'flexDirection': 'column',
                        'justifyContent': 'center',
                        'alignItems': 'center',
                    }),
                ], style={
                    'backgroundImage': f'url({self.app.get_asset_url("corn.jpg")})',
                    'backgroundSize': 'cover',
                    'backgroundPosition': 'center center',
                    'height': '100vh',
                    'position': 'relative',  # Needed to position the overlay correctly
                })]
            elif pathname == "/Corn_Sales":
                return html.Div([
                    html.H1('Corn Sales Report'),
                    dcc.Tabs(id='tabs2', value = 'tab0-1', children= [
                        dcc.Tab(label='Export Contries Sales Analysis', value='tab0-1'),
                        dcc.Tab(label= 'Weekly Sales Analysis', value = 'tab0-2')
                    ]),
                    html.Div(id='tabs2-content')
                ]),
                
            elif pathname == "/Weather":
                return html.Div([
                    html.H1('Weather Analysis Report'),
                    dcc.Tabs(id='tabs0', value = 'tab0-1', children= [
                        dcc.Tab(label='Precipitation Analysis', value='tab0-1'),
                        dcc.Tab(label= 'Temperature Analysis', value = 'tab0-2')
                    ]),
                    html.Div(id='tabs0-content')
                ]),
            
            elif pathname == "/Ethanol":
                return html.Div([
                    html.H1('Ethanol Analysis Report'),
                    dcc.Graph(figure = Ethanol().create_ethanol_plot())
                    
                    ])
            
            elif pathname == '/Indices':
                return html.Div([
                    html.H1('Relevent Indices'),
                    html.Label('Period: '),
                    dcc.RadioItems(
                        id = 'toggle-usd',
                        options = [
                            {'label' : '1y', 'value': '1y'},
                            {'label' : '1m', 'value': '1m'},
                            {'label' : '5d', 'value': '5 days'}
                        ],
                        value = '1y',
                        inline = True,
                    ),
                    html.Div([
                        dcc.Graph(id ='usd-chart'), 
                    ]),
                    html.Div([
                        dcc.Graph(id ='oil-chart'), 
                    ]),
                    html.Div([
                        dcc.Graph(id ='gas-chart'), 
                    ]),
                ])
                
            elif pathname == '/XGBoost_analysis':
                return html.Div(
                    children = [
                    html.H1('Machine Learning with XGBoost'),
                    dcc.Tabs(id='tabs3', value = 'tab3-1',children = [
                        dcc.Tab(label='XGBoost Prediction', value = 'tab3-1'),
                        dcc.Tab(label = 'Feature Importance', value = 'tab3-2'),
                    ]),
                    html.Div(id = 'tabs3-content')
                ])
                
            elif pathname == "/LSTM_analysis":
                return html.Div([
                    html.H1('Machine Learning with LSTM'),
                    dcc.Tabs(id="tabs", value='tab-1', children=[
                        dcc.Tab(label='Analyze Corn Chart', value='tab-1'),
                        dcc.Tab(label='Training LSTM', value='tab-2'),
                        dcc.Tab(label='Forecasting LSTM', value='tab-3'),
                    ]),
                    html.Div(id='tabs-content'),
                ]),
            else:
                return dbc.Jumbotron(
                [
                    html.H1("404: Not found"),
                    html.Hr(),
                    html.P(f"The pathname {pathname} was not recognised..."),
                ]
            )
        
        @self.app.callback(
            Output('tabs2-content', 'children'),
            Input('tabs2', 'value')
         )
        def render_content_two(tab):
            if tab == 'tab0-1':
                fig =EsrAnalysis().create_export_country_plot()
                fig2 = EsrAnalysis().create_top_10_country_plot()
                return  html.Div([
                    dcc.Graph(figure = fig),
                    html.Br(),
                    dcc.Graph(figure = fig2),
                ])

            else:
                fig = EsrAnalysis().create_export_weekly_plot()
                return dcc.Graph(figure = fig)
        
        @self.app.callback(
            Output('tabs0-content', 'children'),
            Input('tabs0', 'value')
         )
        def render_content_zero(tab):
            if tab == 'tab0-1':
                fig = WeatherAnalysis().create_precipitation_plot()
                return  dcc.Graph(figure = fig)

            else:
                fig = WeatherAnalysis().create_temperature_plot()
                return dcc.Graph(figure = fig)

        @self.app.callback(
            Output('usd-chart', 'figure'),
            Input('toggle-usd', 'value'),
        )
        def display_usd_chart(value):
            fig = Indices().analyze_usd_prices(value)
            return fig
        @self.app.callback(
            Output('oil-chart', 'figure'),
            Input('toggle-usd', 'value'),
        )
        def display_oil_chart(value):
            fig = Indices().analyze_oil_prices(value)
            return fig
        @self.app.callback(
            Output('gas-chart', 'figure'),
            Input('toggle-usd', 'value'),
        )
        def display_gas_chart(value):
            fig = Indices().analyze_gas_prices(value)
            return fig
        
        
        @self.app.callback(
            Output('tabs3-content', 'children'),
            Input('tabs3', 'value')
        )
        def xgboost_analysis(value):
            if value == 'tab3-1':
                return html.Div([
                    html.Img(src = self.app.get_asset_url("xgb_chart.png"), style={'display': 'block', 'margin': 'auto', 'height': '60vh'}),
                    html.Img(src = self.app.get_asset_url("xgb_eval.png"), style={'display': 'block', 'margin': 'auto', 'height': '30vh'}),
                    ])
            else:
                return html.Div([
                    html.Img(src = self.app.get_asset_url("xgb_importance.png"), style={'display': 'block', 'margin': 'auto', 'height': '60vh'}),
                ])
        
        
        @self.app.callback(
            Output('tabs-content', 'children'),
            Input('tabs', 'value')
        )
        def render_content(tab):
            if tab == 'tab-1':
                return html.Div([
                    html.Label('Period:'),
                    dcc.RadioItems(
                        id='toggle-rangeslider',
                        options=[{ 'label': "All time", 'value': "All" },
                                { 'label': "5y", 'value': "5y" },
                                { 'label': "1y", 'value': "1y" },
                                { 'label': "6m", 'value': "6m" },
                                { 'label': "1m", 'value': "1m" },
                                { 'label': "last 5 prices", 'value': "5 days" },],
                        value = "All",
                        inline=True                  
                    ),
                    dcc.Graph(id='corn-chart')
                ])
                
            elif tab == 'tab-2':
                return html.Div([
                    html.Label('Optimizer:'),
                    dcc.RadioItems(
                        id='toggle-optimizer',
                        options=[
                            {'label' : 'SGD', 'value': 'SGD'},
                            {'label' : 'Adam', 'value': 'Adam'}
                        ],
                        value = 'SGD',
                        inline=True
                    ),
                    html.Label('Epoch:'),
                    html.Div(
                    dcc.Slider(
                        id = 'toggle-epoch',
                        min=0, max=50, step=5, value = 1,
                        marks={
                            0:'default weights',
                            5: '5 epochs',
                            10: '10 epochs',
                            15: '15 epochs',
                            20: '20 epochs',
                            25: '25 epochs',
                            30: '30 epochs',
                            35: '35 epochs',
                            40: '40 epochs',
                            45: '45 epochs',
                            50: '50 epochs',
                        }),
                        style= {
                            'width' :'80%',
                            'margin' : '1%'}),
                    
                    html.Button('Train', id = 'train',style={
                                'background-color': '#007bff',
                                'color': 'white',
                                'border': 'none',
                                'border-radius': '5px',
                                'padding': '5px 10px',
                                'text-align': 'center',
                                'text-decoration': 'none',
                                'display': 'inline-block',
                                'font-size': '16px',
                                'margin': '4px 2px',
                                'cursor': 'pointer',
                            }),
                    
                    dcc.Graph(id='eval-chart'),
                    dcc.Graph(id ='eval-table'),
                    
                ])
            else:
                return html.Div([
                        html.Label('Forecast Number of Days:'),
                        dcc.Slider(
                            id = 'toggle-days',
                            min = 0, max =30, value = 0, step = 1,
                            marks={
                                0:'0', 5:'5', 10:'10', 15: '15', 20: '20', 25:'25', 30:'30 days'
                            }
                        ),
                        html.Label('Weights:'),
                        dcc.RadioItems(
                            id = 'toggle-weights',
                            options = [
                                {'label' : 'Default weights', 'value' : True},
                                {'label' : 'Train weights', 'value': False}
                            ],
                            value= False,
                            inline = True,
                        ),
                        html.Button('Forecast', id = 'forecast',style={
                                'background-color': '#007bff',
                                'color': 'white',
                                'border': 'none',
                                'border-radius': '5px',
                                'padding': '5px 10px',
                                'text-align': 'center',
                                'text-decoration': 'none',
                                'display': 'inline-block',
                                'font-size': '16px',
                                'margin': '4px 2px',
                                'cursor': 'pointer',
                                }),
                        
                        dcc.Graph(id='forecast-chart'),
                    ])
        
        @self.app.callback(
            Output('corn-chart', 'figure'),
            Input("toggle-rangeslider", "value"),
        )
        def display_corn_chart(value):
            fig = MachineLearning().analyze_stock_prices(value)
            return fig
        @self.app.callback(
            [Output('eval-chart', 'figure'),
             Output('eval-table', 'figure')],
            Input('train', 'n_clicks'),
            [State('toggle-epoch', 'value'),
            State('toggle-optimizer', 'value')]
        )
        def evaluate(n_clicks, num_epoch, optimizer):
            if n_clicks is None:
                return MachineLearning().train_model(0, 'ADAM')
            else :
                return MachineLearning().train_model(num_epoch, optimizer)
    
        @self.app.callback(
            Output('forecast-chart', 'figure'),
            Input('forecast', 'n_clicks'),
            [State('toggle-days', 'value'),
             State('toggle-weights', 'value')]
        )
        def forecast(n_clicks, days, weight):
            if n_clicks is None:
                return no_update
            else:
                fig = MachineLearning().forecast_stock_prices(days, weight)
                return fig
        
    def run(self, debug = False):
        self.app.run_server(debug = debug, port = 8080)
    
 


if __name__ == '__main__':
    app = App()
    app.run(debug = True)