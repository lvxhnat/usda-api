from pydantic import BaseModel
from datetime import datetime

class PSDDataReleaseDates(BaseModel):
    commodityCode: str
    countryCode: str
    marketYear: str
    releaseYear: str
    releaseMonth: str


class PSDRegionsType(BaseModel):
    regionCode: str
    regionName: str


class PSDCommoditiesType(BaseModel):
    commodityCode: str
    commodityName: str


class PSDCountriesType(BaseModel):
    countryCode: str
    countryName: str
    regionCode: str
    gencCode: str


class PSDCommoditiesDataType(BaseModel):
    commodityCode: str
    countryCode: str
    marketYear: str
    calendarYear: str
    month: str
    attributeId: int
    unitId: int
    value: int


class PSDUnitsOfMeasure(BaseModel):
    unitId: str
    unitDescription: str


class PSDCommodityAttributes(BaseModel):
    attributeId: str
    attributeName: str

