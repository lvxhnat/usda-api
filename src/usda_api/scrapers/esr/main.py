"""
USDA Export Sales Reporting (ERS) DATA 
------------------------------------------
USDA's Export Sales Reporting Program monitors U.S. agricultural export sales on a daily and weekly basis. 
Export sales reporting provides a constant stream of up-to-date market information for 40 U.S. agricultural commodities sold abroad.
A single statistic reveals the significance of the program: in a typical year, the program monitors more than 40 percent of total U.S. agricultural exports. 
The program also serves as an early alert on the possible impact foreign sales have on U.S. supplies and prices.
The weekly U.S. Export Sales report is the most currently available source of U.S. export sales data. 
The data is used to analyze the overall level of export demand, determine where markets exist, and assess the relative position of U.S. commodities in foreign markets.
"""

import requests
from datetime import datetime
from typing import Callable, List

from usda_api.scrapers.esr.schemas import (
    ESRRegionsType,
    ESRRegionsCleanedType,
    ESRCountriesType,
    ESRCountriesCleanedType,
    ESRCommoditiesType,
    ESRCommoditiesCleanedType,
    ESRUnitsOfMeasureType,
    ESRUnitsOfMeasureCleanedType,
    ESRCountryExportType,
    ESRCountryExportCleanedType,
    ESRAllCountriesExportType,
    ESRAllCountriesExportCleanedType,
    ESRCountryExportGeneralType,
    ESRCountryExportGeneralCleanedType,
)

API_ID: str = "/api/esr"
BASE_API: str = "https://apps.fas.usda.gov/OpenData"

REGIONS: str = "/regions"
COUNTRIES: str = "/countries"
COMMODITIES: str = "/commodities"
UNITS_OF_MEASURE: str = "/unitsOfMeasure"
DATA_RELEASE_DATES: str = "/datareleasedates"

EXPORTS_ALL_COUNTRIES: Callable = (
    lambda commodityCode, marketYear: f"/exports/commodityCode/{commodityCode}/allCountries/marketYear/{marketYear}"
)
EXPORTS_BY_COUNTRY_CODE: Callable = (
    lambda commodityCode, countryCode, marketYear: f"/exports/commodityCode/{commodityCode}/countryCode/{countryCode}/marketYear/{marketYear}"
)


def generate_request(endpoint: str, /, api_key: str) -> requests.Response:
    HEADERS: str = {"API_KEY": api_key}
    print(endpoint)
    return requests.get(endpoint, headers=HEADERS)


def get_esr_regions(api_key: str) -> List[ESRRegionsCleanedType]:
    ENDPOINT: str = BASE_API + API_ID + REGIONS
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    region_response: List[ESRRegionsType] = response.json()
    return [
        *map(
            lambda entry: {
                "region_id": entry["regionId"],
                "region_name": entry["regionName"].strip(),
            },
            region_response,
        )
    ]


def get_esr_countries(api_key: str) -> List[ESRCountriesCleanedType]:
    ENDPOINT: str = BASE_API + API_ID + COUNTRIES
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    countries_response: List[ESRCountriesType] = response.json()
    return [
        *map(
            lambda entry: {
                "country_code": entry["countryCode"],
                "country_name": entry["countryName"].strip(),
                "country_description": entry["countryDescription"].strip(),
                "region_id": entry["regionId"],
                "genc_code": entry["gencCode"],
            },
            countries_response,
        )
    ]


def get_esr_commodities(api_key: str) -> List[ESRCommoditiesCleanedType]:
    ENDPOINT: str = BASE_API + API_ID + COMMODITIES
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    commodities_response: List[ESRCommoditiesType] = response.json()
    return [
        *map(
            lambda entry: {
                "commodity_code": entry["commodityCode"],
                "commodity_name": entry["commodityName"].strip(),
                "unit_id": entry["unitId"],
            },
            commodities_response,
        )
    ]


def get_esr_unitsofmeasure(api_key: str) -> List[ESRUnitsOfMeasureCleanedType]:
    ENDPOINT: str = BASE_API + API_ID + UNITS_OF_MEASURE
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    unitsofmeasure_response: List[ESRUnitsOfMeasureType] = response.json()
    return [
        *map(
            lambda entry: {
                "unit_id": entry["unitId"],
                "unit_names": entry["unitNames"].strip(),
            },
            unitsofmeasure_response,
        )
    ]


def clean_country_exports(
    response: List[ESRCountryExportGeneralType],
) -> List[ESRCountryExportGeneralCleanedType]:
    return [
        *map(
            lambda entry: {
                "commodity_code": entry["commodityCode"],
                "country_code": entry["countryCode"],
                "weekly_exports": entry["weeklyExports"],
                "accumulated_exports": entry["accumulatedExports"],
                "outstanding_sales": entry["outstandingSales"],
                "gross_new_sales": entry["grossNewSales"],
                "currentMY_net_sales": entry["currentMYNetSales"],
                "currentMY_total_commitment": entry["currentMYTotalCommitment"],
                "nextMY_outstanding_sales": entry["nextMYOutstandingSales"],
                "nextMY_net_sales": entry["nextMYNetSales"],
                "unit_id": entry["unitId"],
                "week_ending_date": datetime.strptime(
                    entry["weekEndingDate"], "%Y-%m-%dT%H:%M:%S"
                ),
            },
            response,
        )
    ]


def get_esr_allcountries_export(
    api_key: str, commodity_code: int, market_year: str
) -> List[ESRAllCountriesExportCleanedType]:
    ENDPOINT: str = (
        BASE_API
        + API_ID
        + EXPORTS_ALL_COUNTRIES(commodityCode=commodity_code, marketYear=market_year)
    )
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    allcountries_response: List[ESRAllCountriesExportType] = response.json()
    return clean_country_exports(allcountries_response)


def get_esr_country_export(
    api_key: str, commodity_code: str, country_code: str, market_year: str
) -> List[ESRCountryExportCleanedType]:
    ENDPOINT: str = (
        BASE_API
        + API_ID
        + EXPORTS_BY_COUNTRY_CODE(
            commodityCode=commodity_code,
            countryCode=country_code,
            marketYear=market_year,
        )
    )
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    country_response: List[ESRCountryExportType] = response.json()
    return clean_country_exports(country_response)
