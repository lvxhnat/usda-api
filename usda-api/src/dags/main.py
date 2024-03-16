from usda_api.scrapers.esr.main import get_esr_regions
from usda_api.scrapers.esr.main import get_esr_countries
from usda_api.scrapers.esr.main import get_esr_commodities
from usda_api.scrapers.esr.main import get_esr_unitsofmeasure
from usda_api.scrapers.esr.main import get_esr_allcountries_export
from usda_api.scrapers.esr.main import get_esr_country_export


from usda_api.scrapers.psd.main import get_psd_countries
from usda_api.scrapers.psd.main import get_psd_unitsofmeasure
from usda_api.scrapers.psd.main import get_psd_commodities
from usda_api.scrapers.psd.main import get_psd_commodityattributes
from usda_api.scrapers.psd.main import get_psd_countryforecast

from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta

    

default_args = {
    'owner' : 'main',
    'start_date' : datetime(2024,1,1),
    'retries': 1,
    'retries_interval': timedelta(minutes=0.5)
}


API='d317a077-50d7-4949-9958-42d7cb97a1d3'
COMMODITY_CODE= '0440000'
COUNTRY = 'US'

#region = get_esr_regions(api)
#country = get_esr_countries(api)
#commodities = get_esr_commodities(api)
#unitofmeasure = get_esr_unitsofmeasure(api)
#for i in country:
#    print(i['country_name'], i['country_code'])
#allcountriesexport = get_esr_allcountries_export(api,401,2023)
#countryexport = get_esr_country_export(api, '401', '5700','2022')
#commodityattribute = get_psd_commodityattributes(api)
country_for = get_psd_countryforecast(API, COMMODITY_CODE, COUNTRY, '2023')
print(country_for[0])