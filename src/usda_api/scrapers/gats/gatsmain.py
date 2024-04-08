
import requests
from datetime import datetime
from typing import Callable, List

from schemas.response import (
    GatsCensusExportDataReleaseDates,
    GatsCensusImportDataReleaseDates,
    GatsUntradeExportDataReleaseDates,
    GatsUntradeImportDataReleaseDates,
    GatsRegionsType,
    GatsCountries,
    GatsCommoditiesCleanedType,
    GatsHS6CommoditiesCleanedType,
    GatsUnitsOfMeasureCleanedType,
    GatsCustomDistricts,
    GatsCensusImportByDate,
    GatsCensusExportByDate,
    GatsCensusReExportByDate,
    GatsCustomDistrictImportByDate,
    GatsCustomDistrictExportByDate,
    GatsCustomDistrictReExportByDate,
    GatsUntradeImportReporterYear
)

API_ID: str = "/api/gats"
BASE_API: str = "https://apps.fas.usda.gov/OpenData"

REGIONS: str = "/regions"
COUNTRIES: str = "/countries"
COMMODITIES: str = "/commodities"
UNITS_OF_MEASURE: str = "/unitsOfMeasure"
DATA_RELEASE_DATES: str = "/dataReleaseDates"

# EXPORTS_ALL_COUNTRIES: Callable = (
#     lambda commodityCode, marketYear: f"/exports/commodityCode/{commodityCode}/allCountries/marketYear/{marketYear}"
# )
# EXPORTS_BY_COUNTRY_CODE: Callable = (
#     lambda commodityCode, countryCode, marketYear: f"/exports/commodityCode/{commodityCode}/countryCode/{countryCode}/marketYear/{marketYear}"
# )


def generate_request(endpoint: str, /, api_key: str) -> requests.Response:
    HEADERS: str = {"API_KEY": api_key}
    print(endpoint)
    return requests.get(endpoint, headers=HEADERS)

def get_gats_census_exports_dataReleaseDates(api_key: str) -> List[GatsCensusExportDataReleaseDates]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/census/data/exports" + DATA_RELEASE_DATES
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    dataReleaseDates_response: List[GatsCensusExportDataReleaseDates] = response.json()
    return [
        *map(
            lambda entry: {
                "statisticalYearMonth": entry["statisticalYearMonth"],
                "productType": entry["productType"],
                "reporterCode": entry["reporterCode"],
                "releaseTimeStamp": entry["releaseTimeStamp"],
            },
            dataReleaseDates_response,
        )
    ]

def get_gats_census_imports_dataReleaseDates(api_key: str) -> List[GatsCensusImportDataReleaseDates]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/census/data/exports" + DATA_RELEASE_DATES
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    dataReleaseDates_response: List[GatsCensusImportDataReleaseDates] = response.json()
    return [
        *map(
            lambda entry: {
                "statisticalYearMonth": entry["statisticalYearMonth"],
                "productType": entry["productTyp"],
                "reporterCode": entry["reporterCode"],
                "releaseTimeStamp": entry["releaseTimeStamp"],
            },
            dataReleaseDates_response,
        )
    ]

def get_gats_untrade_exports_dataReleaseDates(api_key: str) -> List[GatsUntradeExportDataReleaseDates]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/UNTrade/data/exports" + DATA_RELEASE_DATES
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    dataReleaseDates_response: List[GatsUntradeExportDataReleaseDates] = response.json()
    return [
        *map(
            lambda entry: {
                "statisticalYearMonth": entry["statisticalYearMonth"],
                "productType": entry["productTyp"],
                "reporterCode": entry["reporterCode"],
                "releaseTimeStamp": entry["releaseTimeStamp"],
            },
            dataReleaseDates_response,
        )
    ]


def get_gats_untrade_imports_dataReleaseDates(api_key: str) -> List[GatsUntradeImportDataReleaseDates]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/UNTrade/data/imports" + DATA_RELEASE_DATES
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    dataReleaseDates_response: List[GatsUntradeImportDataReleaseDates] = response.json()
    return [
        *map(
            lambda entry: {
                "statisticalYearMonth": entry["statisticalYearMonth"],
                "productType": entry["productType"],
                "reporterCode": entry["reporterCode"],
                "releaseTimeStamp": entry["releaseTimeStamp"],
            },
            dataReleaseDates_response,
        )
    ]

def get_gats_regions(api_key: str) -> List[GatsRegionsType]:
    ENDPOINT: str = BASE_API + API_ID + REGIONS
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    region_response: List[GatsRegionsType] = response.json()
    return [
        *map(
            lambda entry: {
                "region_code": entry["regionCode"],
                "region_name": entry["regionName"].strip(),
            },
            region_response,
        )
    ]


def get_gats_countries(api_key: str) -> List[GatsCountries]:
    ENDPOINT: str = BASE_API + API_ID + COUNTRIES
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    countries_response: List[GatsCountries] = response.json()
    return [
        *map(
            lambda entry: {
                "country_code": entry["countryCode"],
                "region_code": entry["regionCode"].strip(),
                "country_parent_code": entry["countryParentCode"].strip(),
                "country_name": entry["countryName"].strip(),
                "description": entry["description"],
                "is03Code": entry["is03Code"],
                "discountinuedOn": entry["discountinuedOn"],
                "genc_code": entry["gencCode"],
            },
            countries_response,
        )
    ]


def get_gats_commodities(api_key: str) -> List[GatsCommoditiesCleanedType]:
    ENDPOINT: str = BASE_API + API_ID + COMMODITIES
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    commodities_response: List[GatsCommoditiesCleanedType] = response.json()
    return [
        *map(
            lambda entry: {
                "hS10Code": entry["hS10Code"],
                "startDate": entry["startDate"],
                "endDtate": entry["endDtate"],
                "productType": entry["productType"],
                "commodityName": entry["commodityName"],
                "commodityDescription": entry["commodityDescription"],
                "isAgCommodity": entry["isAgCommodity"],
                "censusUOMId1": entry["censusUOMId1"],
                "censusUOMId1": entry["censusUOMId2"],
                "fasConvertedUOMId": entry["fasConvertedUOMId"],
                "fasNonConvertedUOMId": entry["fasNonConvertedUOMId"],
            },
            commodities_response,
        )
    ]

def get_gats_HS6commodities(api_key: str) -> List[GatsHS6CommoditiesCleanedType]:
    ENDPOINT: str = BASE_API + API_ID + '/HS6Commodities'
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    commodities_response: List[GatsHS6CommoditiesCleanedType] = response.json()
    return [
        *map(
            lambda entry: {
                "hS6Code": entry["hS10Code"],
                "commodityName": entry["commodityName"],
                "commodityDescription": entry["commodityDescription"],
                "censusUOMId1": entry["censusUOMId1"],
                "censusUOMId1": entry["censusUOMId2"],
                "fasConvertedUOMId": entry["fasConvertedUOMId"],
                "fasNonConvertedUOMId": entry["fasNonConvertedUOMId"],
            },
            commodities_response,
        )
    ]


def get_gats_unitsofmeasure(api_key: str) -> List[GatsUnitsOfMeasureCleanedType]:
    ENDPOINT: str = BASE_API + API_ID + UNITS_OF_MEASURE
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    unitsofmeasure_response: List[GatsUnitsOfMeasureCleanedType] = response.json()
    return [
        *map(
            lambda entry: {
                "unitOfMeasureId": entry["unitOfMeasureId"],
                "unitOfMeasureCode": entry["unitOfMeasureCode"].strip(),
                "unitOfMeasureName": entry["unitOfMeasureName"],
                "unitOfMeasureDescription": entry["unitOfMeasureDescription"].strip(),
            },
            unitsofmeasure_response,
        )
    ]


def get_gats_customsDistricts(api_key: str) -> List[GatsCustomDistricts]:
    ENDPOINT: str = BASE_API + API_ID + '/customsDistricts'
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    unitsofmeasure_response: List[GatsCustomDistricts] = response.json()
    return [
        *map(
            lambda entry: {
                "customsDistrictId": entry["customsDistrictId"],
                "customsDistictName": entry["customsDistictName"].strip(),
                "customsDistictRegionCode": entry["customsDistictRegionCode"],
                "customsDistictRegionName": entry["customsDistictRegionName"].strip(),
            },
            unitsofmeasure_response,
        )
    ]

def get_gats_censusImportsByDate(api_key: str, partnerCode:str, year:int, month:int) -> List[GatsCensusImportByDate]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/censusImports/partnerCode/"  + partnerCode + \
                    +"/year/" + str(year) + \
                    + "/month/" + str(month) 
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    commodities_data_reponse: List[GatsCensusImportByDate] = response.json()
    return [
        *map(
            lambda entry: {
                "consumptionQuantity1": entry["consumptionQuantity1"],
                "consumptionQuantity2": entry["consumptionQuantity2"],
                "consumptionValue": entry["consumptionValue"],
                "consumptionCIFValue": entry["consumptionCIFValue"],
                "cifValue": entry["cifValue"],
                "date": entry["date"],
                "countryCode": entry["countryCode"],
                "hS10Code": entry["hS10Code"],
                "censusUOMId1": entry["censusUOMId1"],
                "censusUOMId2": entry["censusUOMId2"],
                "fasConvertedUOMId": entry["fasConvertedUOMId"],
                "fasNonConvertedUOMId": entry["fasNonConvertedUOMId"],
                "quantity1": entry["quantity1"],
                "quantity2": entry["quantity2"],
                "value": entry["value"],
            },
            commodities_data_reponse,
        )
    ]

def get_gats_censusExportsByDate(api_key: str, partnerCode:str, year:int, month:int) -> List[GatsCensusExportByDate]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/censusExports/partnerCode/"  + partnerCode + \
                    "/year/" + str(year) + \
                     "/month/" + str(month) 
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    commodities_data_reponse: List[GatsCensusExportByDate] = response.json()
    return [
        *map(
            lambda entry: {
                "date": entry["date"],
                "countryCode": entry["countryCode"],
                "hS10Code": entry["hS10Code"],
                "censusUOMId1": entry["censusUOMId1"],
                "censusUOMId2": entry["censusUOMId2"],
                "fasConvertedUOMId": entry["fasConvertedUOMId"],
                "fasNonConvertedUOMId": entry["fasNonConvertedUOMId"],
                "quantity1": entry["quantity1"],
                "quantity2": entry["quantity2"],
                "value": entry["value"],
            },
            commodities_data_reponse,
        )
    ]

def get_gats_censusReExportsByDate(api_key: str, partnerCode:str, year:int, month:int) -> List[GatsCensusReExportByDate]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/censusReExports/partnerCode/"  + partnerCode + \
                    +"/year/" + str(year) + \
                    + "/month/" + str(month) 
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    commodities_data_reponse: List[GatsCensusReExportByDate] = response.json()
    return [
        *map(
            lambda entry: {
                "date": entry["date"],
                "countryCode": entry["countryCode"],
                "hS10Code": entry["hS10Code"],
                "censusUOMId1": entry["censusUOMId1"],
                "censusUOMId2": entry["censusUOMId2"],
                "fasConvertedUOMId": entry["fasConvertedUOMId"],
                "fasNonConvertedUOMId": entry["fasNonConvertedUOMId"],
                "quantity1": entry["quantity1"],
                "quantity2": entry["quantity2"],
                "value": entry["value"],
            },
            commodities_data_reponse,
        )
    ]

def get_gats_customDistrictImportsByDate(api_key: str, partnerCode:str, year:int, month:int) -> List[GatsCustomDistrictImportByDate]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/customDistrictImports/partnerCode/"  + partnerCode + \
                    +"/year/" + str(year) + \
                    + "/month/" + str(month) 
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    commodities_data_reponse: List[GatsCustomDistrictImportByDate] = response.json()
    return [
        *map(
            lambda entry: {
                "customsDistrictId": entry["customsDistrictId"],
                "consumptionQuantity1": entry["consumptionQuantity1"],
                "consumptionQuantity2": entry["consumptionQuantity2"],
                "consumptionValue": entry["consumptionValue"],
                "consumptionCIFValue": entry["consumptionCIFValue"],
                "cifValue": entry["cifValue"],
                "date": entry["date"],
                "countryCode": entry["countryCode"],
                "hS10Code": entry["hS10Code"],
                "censusUOMId1": entry["censusUOMId1"],
                "censusUOMId2": entry["censusUOMId2"],
                "fasConvertedUOMId": entry["fasConvertedUOMId"],
                "fasNonConvertedUOMId": entry["fasNonConvertedUOMId"],
                "quantity1": entry["quantity1"],
                "quantity2": entry["quantity2"],
                "value": entry["value"],
            },
            commodities_data_reponse,
        )
    ]

def get_gats_customDistrictExportsByDate(api_key: str, partnerCode:str, year:int, month:int) -> List[GatsCustomDistrictExportByDate]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/customsDistrictExports/partnerCode/"  + partnerCode + \
                    +"/year/" + str(year) + \
                    + "/month/" + str(month) 
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    commodities_data_reponse: List[GatsCustomDistrictExportByDate] = response.json()
    return [
        *map(
            lambda entry: {
                "customsDistrictId": entry["customsDistrictId"],
                "date": entry["date"],
                "countryCode": entry["countryCode"],
                "hS10Code": entry["hS10Code"],
                "censusUOMId1": entry["censusUOMId1"],
                "censusUOMId2": entry["censusUOMId2"],
                "fasConvertedUOMId": entry["fasConvertedUOMId"],
                "fasNonConvertedUOMId": entry["fasNonConvertedUOMId"],
                "quantity1": entry["quantity1"],
                "quantity2": entry["quantity2"],
                "value": entry["value"],
            },
            commodities_data_reponse,
        )
    ]

def get_gats_customDistrictReExportsByDate(api_key: str, partnerCode:str, year:int, month:int) -> List[GatsCustomDistrictReExportByDate]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/customsDistrictReExports/partnerCode/"  + partnerCode + \
                    +"/year/" + str(year) + \
                    + "/month/" + str(month) 
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    commodities_data_reponse: List[GatsCustomDistrictReExportByDate] = response.json()
    return [
        *map(
            lambda entry: {
                "customsDistrictId": entry["customsDistrictId"],
                "date": entry["date"],
                "countryCode": entry["countryCode"],
                "hS10Code": entry["hS10Code"],
                "censusUOMId1": entry["censusUOMId1"],
                "censusUOMId2": entry["censusUOMId2"],
                "fasConvertedUOMId": entry["fasConvertedUOMId"],
                "fasNonConvertedUOMId": entry["fasNonConvertedUOMId"],
                "quantity1": entry["quantity1"],
                "quantity2": entry["quantity2"],
                "value": entry["value"],
            },
            commodities_data_reponse,
        )
    ]

def get_gats_untrade_imports_reporteryear(api_key: str, reporterCode:str, year:int,) -> List[GatsUntradeImportReporterYear]:
    ENDPOINT: str = BASE_API + API_ID + \
                    "/UNTradeImports/reporterCode/" + reporterCode + "/year/"+ str(year)
    response: requests.Response = generate_request(ENDPOINT, api_key=api_key)
    dataReleaseDates_response: List[GatsUntradeImportReporterYear] = response.json()
    return [
        *map(
            lambda entry: {
                "year": entry["year"],
                "reporterCountryCode": entry["reporterCountryCode"],
                "partnerCountryCode": entry["partnerCountryCode"],
                "hS6Code": entry["hS10Code"],
                "censusUOMId1": entry["censusUOMId1"],
                "censusUOMId2": entry["censusUOMId2"],
                "fasConvertedUOMId": entry["fasConvertedUOMId"],
                "fasNonConvertedUOMId": entry["fasNonConvertedUOMId"],
                "quantity1": entry["quantity1"],
                "quantity2": entry["quantity2"],
                "value": entry["value"],
            },
            dataReleaseDates_response,
        )
    ]


#DID NOT INCLUDE UnTradeExports & UnTradeReExports because there's no data (Last 2 Gets)


if __name__ == '__main__':
    EIA_API_KEY="Q2QFNdOk3rcYcN3Kdeavmx73tblUnZJg4uiter1W"
    USDA_FAS_API_KEY_0="f5458abd-198d-402b-b75e-3ce48527b0d2"

    #res = get_gats_census_exports_dataReleaseDates(USDA_FAS_API_KEY_0)
    #res = get_gats_censusExportsByDate(USDA_FAS_API_KEY_0,'MX',2017,1)
    #print(res)
    #print(res[0])
    #CORN HS10 code starts with 1005
    #print(res.filter(lambda x: x['hS10Code'][:4] == '1005'))
    totalexportvalue = 0
    for i in range(12):
        res = get_gats_censusExportsByDate(USDA_FAS_API_KEY_0,'MX',2017,i+1)
        filteredcornlist = list(filter(lambda x: x['hS10Code'][:4] == '1005', res))
        totalexportvalue += sum(entry['value'] for entry in filteredcornlist)
    print(totalexportvalue)
