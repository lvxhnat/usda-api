from pydantic import BaseModel
from datetime import datetime


class ESRRegionsType(BaseModel):
    regionId: int
    regionName: str


class ESRRegionsCleanedType(BaseModel):
    region_id: int
    region_name: str


class ESRCountriesType(BaseModel):
    countryCode: int
    countryName: str
    countryDescription: str
    regionId: int
    gencCode: str


class ESRCountriesCleanedType(BaseModel):
    country_code: int
    country_name: str
    country_description: str
    region_id: int
    genc_code: str


class ESRCommoditiesType(BaseModel):
    commodityCode: int
    commodityName: str
    unitId: int


class ESRCommoditiesCleanedType(BaseModel):
    commodity_code: int
    commodity_name: str
    unit_id: int


class ESRUnitsOfMeasureType(BaseModel):
    unitId: str
    unitNames: str


class ESRUnitsOfMeasureCleanedType(BaseModel):
    unit_id: str
    unit_names: str


class ESRCountryExportGeneralType(BaseModel):
    commodityCode: int
    countryCode: int
    weeklyExports: int
    accumulatedExports: int
    outstandingSales: int
    grossNewSales: int
    currentMYNetSales: int
    currentMYTotalCommitment: int
    nextMYOutstandingSales: int
    nextMYNetSales: int
    unitId: int
    weekEndingDate: str  # '2023-04-27T00:00:00' -- %Y-%m-%dT%H:%M:%S


class ESRCountryExportGeneralCleanedType(BaseModel):
    commodity_code: int
    country_code: int
    weekly_exports: int
    accumulated_exports: int
    outstanding_sales: int
    gross_new_sales: int
    currentMY_net_sales: int
    currentMY_total_commitment: int
    nextMY_outstanding_sales: int
    nextMY_net_sales: int
    unit_id: int
    week_ending_date: datetime  # '2023-04-27T00:00:00' -- %Y-%m-%dT%H:%M:%S


class ESRAllCountriesExportType(ESRCountryExportGeneralType):
    pass


class ESRCountryExportType(ESRCountryExportGeneralType):
    pass


class ESRAllCountriesExportCleanedType(ESRCountryExportGeneralCleanedType):
    pass


class ESRCountryExportCleanedType(ESRCountryExportGeneralCleanedType):
    pass
