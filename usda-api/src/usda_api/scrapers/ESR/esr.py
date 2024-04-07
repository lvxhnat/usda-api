import re
import os
import ast
import requests
import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, MetaData, PrimaryKeyConstraint, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

class ESR:
    """ ESR Data API - United States Weekly Export Sales of Agricultural Commodity Data

    Returns a set of records with Commodity Information. 
    Use it to associate Commodity Name with Commodity Data records obtained by querying Commodity Data End point

    # Reference documentation: https://apps.fas.usda.gov/opendataweb/home
    """

    def __init__(self):
        self.unit_of_measure = "Metric Tons"
        self.PARAMS = {"API_KEY": "f5458abd-198d-402b-b75e-3ce48527b0d2"}
        self.url = f"https://apps.fas.usda.gov/OpenData"

        commodities_response = requests.get(
            url=self.url + "/api/esr/commodities", headers=self.PARAMS)
        commodities_for_query = pd.DataFrame.from_dict(
            ast.literal_eval(commodities_response.text))
        
        self.commodity_name_to_id = commodities_for_query.set_index(
            "commodityName")[['commodityCode']].to_dict()['commodityCode']
        self.commodity_id_to_name = commodities_for_query.set_index(
            "commodityCode")[['commodityName']].to_dict()['commodityName']
        
        self.engine = create_engine('postgresql://postgres:Password*1@35.239.18.20/jy')


    def get_commodity_id(self, commodity) -> np.int64:
        """ Retrieve commodity id that we can use for querying given the name of the agricultural product
        Parameters
        ----------    
        commodity    : The commodity name based on the printed possible available inputs given above 

        Example Usage
        ----------   
        >>> commodity = ESR()
        >>> commodity.get_commodity_id('Corn')
        401 
        """

        return self.commodity_name_to_id[commodity]

    def available_countries_for_query(self) -> pd.DataFrame:
        """ Returns a set of records with Countries and their corresponding Regions Codes the Country belongs to. 
        Use it to associate Country Name with Commodity Data records obtained by querying Commodity Data End point.

        Example Usage
        ----------   
        >>> commodity = ESR()
        >>> commodity.available_countries_for_query().head()

        countryCode	 countryName	countryDescription	    regionId	gencCode
        1	          EUROPEAN	    EUROPEAN UNION - 27	        1	      null
        2	           UNKNOWN	         UNKNOWN	            99	       AX1
        1010	       GREENLD	        GREENLAND               11	       GRL
        1220	        CANADA	          CANADA                11	       CAN
        1610	        MIGUEL	   ST. PIERRE AND MIQUELON      11	      null
        ...
        """

        countries_endpoint = "/api/esr/countries"

        countries_response = requests.get(
            url=self.url + countries_endpoint, headers=self.PARAMS).text

        return pd.DataFrame.from_dict(ast.literal_eval(countries_response.replace("null", '"null"')))

    def extract_esr(self,
                    commodity_code: int = 401,
                    market_year: int = datetime.datetime.now().year) -> pd.DataFrame:
        """ Given Commodity Code (Ex: 104 for Wheat - White ) and market year (Ex: 2017) this API End point will return a list of US Export records of White Wheat to all applicable countries from USA for the given Market Year. 
        Please see DataReleaseDates end point to get a list of all Commodities and the corresponding Market Year data.
        || HIGHLIGHT: These numbers are export records of commodity to applicable countries **

        Data Release Frequency: Bi-Weekly

        Parameters
        ----------    
        commodity_code    : commodity code
        market_year       : year we want, 2019, 2020, 2021

        Output
        ----------   
        Output       : pandas dataframe; containing 'commodity', 'commodityCode', 'country', 'countryCode', 'weeklyExports', 'accumulatedExports','outstandingSales', 'grossNewSales', 'currentMYNetSales', 'currentMYTotalCommitment', 'nextMYOutstandingSales', 'nextMYNetSales', 'unitId', 'weekEndingDate'

        Example Usage
        ----------   
        >>> commodity = ESR()
        >>> commodity.countries_export_to_usa(commodityCode = 401)

        commodity	commodityCode	country			countryCode		weeklyExports	accumulatedExports	outstandingSales	grossNewSales	currentMYNetSales	currentMYTotalCommitment	nextMYOutstandingSales	nextMYNetSales	unitId			date
            Corn		401			CANADA				1220			5599			5599				84173				36095			-7244					89772							0					0			1		2019-09-05
            Corn		401			MEXICO				2010			212010			212010				3407428				495420			193847					3619438							60000				0			1		2019-09-05
            Corn		401			GUATEMALA			2050			0				0					314990				20931			7564					314990							0					0			1		2019-09-05
            Corn		401			EL SALVADOR			2110			0				0					85254				15300			10235					85254							0					0			1		2019-09-05
            Corn		401			HONDURAS			2150			28435			28435				171196				54530			32274					199631							0					0			1		2019-09-05
        ...

        || NOTES: currentMYTotalCommitment - Signed Contracts for buying the commodity 
        """

        availableCountries = self.available_countries_for_query()
        mapCountries = availableCountries.set_index(
            "countryCode")[['countryDescription']].to_dict()['countryDescription']

        countriesExport_endpoint = f"/api/esr/exports/commodityCode/{commodity_code}/allCountries/marketYear/{market_year}"

        export_response = requests.get(
            url=self.url + countriesExport_endpoint, headers=self.PARAMS).text
        
        
        exportsdf = pd.DataFrame(ast.literal_eval(export_response))
        

        exportsdf['Commodity'] = exportsdf.commodityCode.apply(
            lambda x: self.commodity_id_to_name[x])
        exportsdf['Country'] = exportsdf.countryCode.apply(
            lambda x: mapCountries[x])
        exportsdf = exportsdf[['Commodity', 'Country', 'weeklyExports', 'weekEndingDate']]

        exportsdf.Country = exportsdf.Country.apply(
            lambda x: x.strip())  # Formatting dates and country spacing
        exportsdf['Date'] = exportsdf.weekEndingDate.apply(
            lambda x: datetime.datetime.strptime(re.findall("....-..-..", x)[0], "%Y-%m-%d"))
        exportsdf = exportsdf.drop(columns=['weekEndingDate'])
        
        return exportsdf
    
    def extract_and_transform_esr(self):
        df = None
        for i in ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']:
            export_df = self.extract_esr(market_year=i)
            if df is None:
                df = export_df
                continue
            else:
                df = pd.concat([df, export_df], axis= 0)
        os.makedirs('./data/extract_esr', exist_ok= True)
        path= './data/extract_esr/extract_esr.csv'
        df.to_csv(path, index= False)
        return path
        
    def extract_and_transform_esr_weekly(self):
        export_df = self.extract_esr()
        export_df.Date = pd.to_datetime(export_df.Date)
        os.makedirs('./data/extract_esr_weekly', exist_ok=True)
        path = './data/extract_esr_weekly/extract_esr_weekly.csv'
        today = datetime.date.today()
        week_prior = today - datetime.timedelta(weeks=1)
        export_df_weekly = export_df[export_df['Date'] >= str(week_prior)]
        export_df_weekly.to_csv(path, index = False)
        return path
    
    def load_esr(self, path):
        metadata = MetaData()
        columns = [
            Column('Date', Date, primary_key = True),
            Column('Country', String, primary_key=True),
            Column('Commodity', String),
            Column('weeklyExports', Integer),
        ]

        table = Table('esr', metadata, *columns, PrimaryKeyConstraint('Date', 'Country'))
        
        metadata.create_all(self.engine)

        data_frame = pd.read_csv(path, parse_dates=["Date"], index_col=False)
        data_frame.Date = pd.to_datetime(data_frame.Date, utc = True)
        data_frame.Date = data_frame.Date.dt.date
        data_frame = data_frame[~data_frame.duplicated(subset=['Date', 'Country'], keep ='last')]
        data_frame.to_sql('esr', self.engine, if_exists='append', index =False)
        return 'esr'
    
    def load_esr_weekly(self, path):
        metadata = MetaData()
        table = Table('esr', metadata, autoload=True, autoload_with=self.engine)
        data_frame = pd.read_csv(path, parse_dates=["Date"], index_col=False)
        data_frame.Date = pd.to_datetime(data_frame.Date, utc = True)
        data_frame.Date = data_frame.Date.dt.date
        data_to_insert = data_frame.to_dict('records')
        with self.engine.connect() as conn:
            with conn.begin():
                for data in data_to_insert:
                    upsert_stmt = insert(table).values(**data).\
                        on_conflict_do_update(
                            index_elements= ['Date', 'Country'],
                            set_={c.name: c for c in insert(table).excluded if c.name not in ['Date', 'Country']}
                        )
                    conn.execute(upsert_stmt)
        return 'esr'
        
    def query_esr_all(self):
        all_data = self.engine.execute("""
                                       SELECT * FROM esr
                                       """)
        df = pd.DataFrame(all_data.fetchall(), columns = all_data.keys())
        os.makedirs('./data/Esr_quote', exist_ok=True)
        df.to_csv('./data/Esr_quote/esr_quote.csv', index = False)
        return './data/Esr_quote/esr_quote.csv'
    
    def query_esr_yearly(self):
        all_data = self.engine.execute("""
                                       SELECT * FROM esr
                                       WHERE "Date" BETWEEN (NOW() - INTERVAL '1 year')::DATE AND NOW()::DATE
                                       """)
        df = pd.DataFrame(all_data.fetchall(), columns = all_data.keys())
        os.makedirs('./data/Esr_quote', exist_ok = True)
        df.to_csv('./data/Esr_quote/esr_quote_yearly.csv', index = False)
        return './data/Esr_quote/esr_quote_yearly.csv'
        
            

  
    
if __name__ == '__main__':
    esr = ESR()
    #path = './extract_esr/extract_esr.csv' #esr.extract_and_transform_esr()
    #esr.load_esr(path)
    #esr.query_esr_all()
    esr.query_esr_yearly()