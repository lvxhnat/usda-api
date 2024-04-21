import requests
from datetime import datetime
from typing import Callable, List
import pandas as pd

from usda_api.scrapers.psd.schemas.response import (
    PSDDataReleaseDates,
    PSDRegionsType, 
    PSDCommoditiesType,
    PSDCountriesType,
    PSDCommoditiesDataType,
    PSDUnitsOfMeasure,
    PSDCommodityAttributes,
)

class PSD:
    def __init__(self, api_key):
        self.api_key: str = api_key
        self.api_ids: dict = {
            "API_ID": "/api/psd",
            "BASE_API": "https://apps.fas.usda.gov/OpenData",
            "REGIONS": "/regions",
            "COUNTRIES": "/countries",
            "COMMODITIES": "/commodities",
            "UNITS_OF_MEASURE": "/unitsOfMeasure",
            "DATA_RELEASE_DATES": "/dataReleaseDates",
            "COMMODITY_ATTRIBUTES": "/commodityAttributes"
        }

    def generate_request(self, endpoint: str, api_key: str) -> requests.Response:
        HEADERS: str = {"API_KEY": api_key}
        print(endpoint)
        return requests.get(endpoint, headers=HEADERS)

    def get_psd_dataReleaseDates(self, commodity_code:str) -> List[PSDDataReleaseDates]:
        BASE_API = self.api_ids['BASE_API']
        API_ID = self.api_ids['API_ID']
        DATA_RELEASE_DATES = self.api_ids['DATA_RELEASE_DATES']
        ENDPOINT: str = BASE_API + API_ID + \
                        "/commodity/" + commodity_code + DATA_RELEASE_DATES
        
        response: requests.Response = self.generate_request(endpoint=ENDPOINT, api_key=self.api_key)
        dataReleaseDates_response: List[PSDDataReleaseDates] = response.json()
        return [
            *map(
                lambda entry: {
                    "commodity_code": entry["commodityCode"],
                    "country_code": entry["countryCode"],
                    "market_year": entry["marketYear"],
                    "release_year": entry["releaseYear"],
                    "release_month": entry["releaseMonth"],
                },
                dataReleaseDates_response,
            )
        ]

    def get_psd_regions(self) -> List[PSDRegionsType]:
        BASE_API = self.api_ids['BASE_API']
        API_ID = self.api_ids['API_ID']
        REGIONS = self.api_ids['REGIONS']
        ENDPOINT: str = BASE_API + API_ID + REGIONS

        response: requests.Response = self.generate_request(endpoint=ENDPOINT, api_key=self.api_key)
        region_response: List[PSDRegionsType] = response.json()
        return [
            *map(
                lambda entry: {
                    "region_code": entry["regionCode"],
                    "region_name": entry["regionName"].strip(),
                },
                region_response,
            )
        ]


    def get_psd_countries(self) -> List[PSDCountriesType]:
        BASE_API = self.api_ids['BASE_API']
        API_ID = self.api_ids['API_ID']
        COUNTRIES = self.api_ids['COUNTRIES']
        ENDPOINT: str = BASE_API + API_ID + COUNTRIES

        response: requests.Response = self.generate_request(endpoint=ENDPOINT, api_key=self.api_key)
        country_response: List[PSDCountriesType] = response.json()
        return [
            *map(
                lambda entry: {
                    "country_code": entry["countryCode"],
                    "country_name": entry["countryName"].strip(),
                    "region_code": entry["countryCode"].strip(),
                    "genc_code": entry["gencCode"],
                },
                country_response,
            )
        ]
    
    def extract_countries_data(self):
        #helper to turn data into a dictionary
        def get_psd_countries_dict(countries):
            dict = {}
            for entry in countries:
                country_name = entry['country_name']
                country_info = {
                    'country_code': entry['country_code'],
                    'region_code': entry['region_code'],
                    'genc_code': entry['genc_code']
                }
                dict[country_name] = country_info
            return dict
        countries = self.get_psd_countries()
        country_dict = get_psd_countries_dict(countries)
        return country_dict

    def get_psd_commodities(self) -> List[PSDCommoditiesType]:
        BASE_API = self.api_ids['BASE_API']
        API_ID = self.api_ids['API_ID']
        COMMODITIES = self.api_ids['COMMODITIES']
        ENDPOINT: str = BASE_API + API_ID + COMMODITIES
        print(self.api_key)
        response: requests.Response = self.generate_request(endpoint=ENDPOINT, api_key=self.api_key)
        commodities_reponse: List[PSDCommoditiesType] = response.json()
        return [
            *map(
                lambda entry: {
                    "commodity_code": entry["commodityCode"],
                    "commodity_name": entry["commodityName"].strip(),
                },
                commodities_reponse,
            )
        ]
    
    def extract_commodities_data(self):
        #helper to turn data into a dictionary
        def get_psd_commodity_code_dict(commodities):
            dict = {}
            for entry in commodities:
                dict[entry['commodity_name']] = entry['commodity_code']
            return dict
        commodities = self.get_psd_commodities()
        commodity_dict = get_psd_commodity_code_dict(commodities)
        return commodity_dict


    def get_psd_commodities_data(self, commodity_code:str, country_code:str, market_year:str) -> List[PSDCommoditiesDataType]:
        BASE_API = self.api_ids['BASE_API']
        API_ID = self.api_ids['API_ID']
        ENDPOINT: str = BASE_API + API_ID + \
                        "/commodity/"  + commodity_code + \
                        "/country/" + country_code + \
                        "/year/" + market_year
        
        response: requests.Response = self.generate_request(endpoint=ENDPOINT, api_key=self.api_key)
        commodities_data_reponse: List[PSDCommoditiesDataType] = response.json()
        return [
            *map(
                lambda entry: {
                    "commodity_code": entry["commodityCode"],
                    "country_code": entry["countryCode"],
                    "market_year": entry["marketYear"],
                    "calendar_year": entry["calendarYear"],
                    "month": entry["month"],
                    "attribute_id": entry["attributeId"],
                    "unit_id": entry["unitId"],
                    "value": entry["value"],
                },
                commodities_data_reponse,
            )
        ]
    
    def get_psd_unitsOfMeasure(self) -> List[PSDUnitsOfMeasure]:
        BASE_API = self.api_ids['BASE_API']
        API_ID = self.api_ids['API_ID']
        UNITS_OF_MEASURE = self.api_ids['UNITS_OF_MEASURE']
        ENDPOINT: str = BASE_API + API_ID + UNITS_OF_MEASURE

        response: requests.Response = self.generate_request(endpoint=ENDPOINT, api_key=self.api_key)
        unitsOfMeasure_response: List[PSDUnitsOfMeasure] = response.json()
        return [
            *map(
                lambda entry: {
                    "unit_id": entry["unitId"],
                    "unit_description": entry["unitDescription"].strip(),
                },
                unitsOfMeasure_response,
            )
        ] 
    
    def extract_unitsOfMeasure(self):
        return pd.DataFrame(self.get_psd_unitsOfMeasure())

    def get_psd_commodityAttributes(self) -> List[PSDCommodityAttributes]:
        BASE_API = self.api_ids['BASE_API']
        API_ID = self.api_ids['API_ID']
        COMMODITY_ATTRIBUTES = self.api_ids['COMMODITY_ATTRIBUTES']
        ENDPOINT: str = BASE_API + API_ID + COMMODITY_ATTRIBUTES

        response: requests.Response = self.generate_request(endpoint=ENDPOINT, api_key=self.api_key)
        commodityAttributes_response: List[PSDCommodityAttributes] = response.json()
        return [
            *map(
                lambda entry: {
                    "attribute_id": entry["attributeId"],
                    "attribute_name": entry["attributeName"].strip(),
                },
                commodityAttributes_response,
            )
        ] 
    
    def extract_commodityAttributes(self):
        return pd.DataFrame(self.get_psd_commodityAttributes())


if __name__ == '__main__':
    USDA_FAS_API_KEY_0="f5458abd-198d-402b-b75e-3ce48527b0d2"
    psd = PSD(USDA_FAS_API_KEY_0)
    commods = psd.get_psd_regions()
    print(commods[:3])