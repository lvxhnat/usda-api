import requests
from datetime import datetime
from typing import Callable, List

from usda_api.scrapers.psd_old.schemas.response import (
    PSDCountriesType,
    PSDCountriesCleanedType,
    PSDCommoditiesType,
    PSDCommoditiesCleanedType,
    PSDUnitsOfMeasureType,
    PSDUnitsOfMeasureCleanedType,
    PSDCommoditiesAttributesType,
    PSDCommoditiesAttributesCleanedType,
    PSDCountryCommoditiesForecastType,
    PSDCountryCommoditiesForecastCleanedType,
    
)

API_ID: str = "/api/psd"
BASE_API: str = "https://apps.fas.usda.gov/OpenData"


COUNTRIES: str = "/countries"
UNITS_OF_MEASURE: str = "/unitsOfMeasure"
COMMODITIES: str = "/commodities"
COMMODITY_ATTRIBUTES: str = "/commodityAttributes"
COUNTRY_FORCAST: Callable = (
    lambda commodityCode, countryCode, marketYear: f"/commodity/{commodityCode}/country/{countryCode}/year/{marketYear}"
)

def generate_request(endpoint: str, /, api_key:str) -> requests.Response:
    HEADERS: str = {"API_KEY": api_key}
    return requests.get(endpoint, headers = HEADERS)

def get_psd_countries(api_key: str) -> List[PSDCountriesCleanedType]:
    ENDPOINT: str = BASE_API + API_ID + COUNTRIES
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    countries_response: List[PSDCountriesType] = response.json()
    return [
        *map(
            lambda entry:{
                'country_code': entry['countryCode'],
                'country_name': entry['countryName'],
                'region_code': entry['regionCode'],
                'genc_code': entry['gencCode']
            }, 
            countries_response
        )
    ]
        
def get_psd_unitsofmeasure(api_key: str)-> List[PSDUnitsOfMeasureCleanedType]:
    ENDPOINT: str = BASE_API +API_ID + UNITS_OF_MEASURE
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    units_of_measure_response: List[PSDUnitsOfMeasureType] = response.json()
    return [
        *map(
            lambda entry: {
                'unit_id': entry['unitId'],
                'unit_description': entry['unitDescription']
            }
            ,units_of_measure_response
        )
    ]

def get_psd_commodities(api_key: str) -> List[PSDCommoditiesCleanedType]:
    ENDPOINT: str = BASE_API + API_ID + COMMODITIES
    response: requests.Response = generate_request(ENDPOINT, api_key= api_key)
    commodities_response: List[PSDCommoditiesType] = response.json()
    return [
        *map(
            lambda entry: {
                'commodity_code': entry['commodityCode'],
                'commodity_name': entry['commodityName']
            }, 
            commodities_response
        )
    ]
    
def get_psd_commodityattributes(api_key: str) -> List[PSDCommoditiesAttributesCleanedType]:
    ENDPOINT: str = BASE_API + API_ID + COMMODITY_ATTRIBUTES
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    commodity_attributes_response: List[PSDCommoditiesAttributesType] = response.json()
    return [
        *map(
            lambda entry: {
                'attribute_id':entry['attributeId'],
                'attribute_name':entry['attributeName']
            }
            ,commodity_attributes_response
        )
    ]
    
def get_psd_countryforecast(
    api_key: str, commodity_code: str, country_code: str, year: int
    ) -> List[PSDCountryCommoditiesForecastCleanedType]:
        ENDPOINT: str = (
            BASE_API+
            API_ID+
            COUNTRY_FORCAST(commodity_code, country_code, year)
        )
        response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
        country_forcast_response: List[PSDCountryCommoditiesForecastType] = response.json()
        return [
            *map(lambda entry: {
                'commodity_code':entry['commodityCode'],
                'country_code':entry['countryCode'],
                'market_year':entry['marketYear'],
                'month':entry['month'],
                'attribute_id':entry['attributeId'],
                'unit_id':entry['unitId'],
                'value':entry['value']
            },
            country_forcast_response)
        ]
    
    

    
