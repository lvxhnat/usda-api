from pydantic import BaseModel
from datetime import datetime


class GatsCensusExportDataReleaseDates(BaseModel):
    statisticalYearMonth: str
    productType: str
    reporterCode: str
    releaseTimeStamp: str

class GatsCensusImportDataReleaseDates(BaseModel):
    statisticalYearMonth: str
    productType: str
    reporterCode: str
    releaseTimeStamp: str

class GatsUntradeExportDataReleaseDates(BaseModel):
    statisticalYearMonth: str
    productType: str
    reporterCode: str
    releaseTimeStamp: str

class GatsUntradeImportDataReleaseDates(BaseModel):
    statisticalYearMonth: str
    productType: str
    reporterCode: str
    releaseTimeStamp: str

class GatsRegionsType(BaseModel):
    regionCode: str
    regionName: str

class GatsCountries(BaseModel):
    countryCode: str
    regionCode: str
    countryParentCode: str
    countryName: str
    description: str
    isO3Code: str
    discontinuedOn: str
    gencCode: str

class GatsCountries(BaseModel):
    countryCode: str
    regionCode: str
    countryParentCode: str
    countryName: str
    description: str
    isO3Code: str
    discontinuedOn: str
    gencCode: str

class GatsCommoditiesCleanedType(BaseModel):
    hS10Code: str
    startDate: str
    endDtate: str
    productType: str
    commodityName: str
    commodityDescription: str
    isAgCommodity: str
    censusUOMId1: str
    censusUOMId2: str
    fasConvertedUOMId: str
    fasNonConvertedUOMId: str

class GatsHS6CommoditiesCleanedType(BaseModel):
    hS6Code: str
    commodityName: str
    commodityDescription: str
    censusUOMId1: str
    censusUOMId2: str
    fasConvertedUOMId: str
    fasNonConvertedUOMId: str

class GatsUnitsOfMeasureCleanedType(BaseModel):
    unitOfMeasureId: str
    unitOfMeasureCode: str
    unitOfMeasureName: str
    unitOfMeasureDescription: str
    

class GatsCustomDistricts(BaseModel):
    customsDistrictId: str
    customsDistictName: str
    customsDistictRegionCode: str
    customsDistictRegionName: str
                
class GatsCensusImportByDate(BaseModel):
    consumptionQuantity1: int
    consumptionQuantity2: int
    consumptionValue: int
    consumptionCIFValue: int
    cifValue: int
    date: str
    countryCode: str
    hS10Code: str
    censusUOMId1: int
    censusUOMId2: int
    fasConvertedUOMId: int
    fasNonConvertedUOMId: int
    quantity1: int
    quantity2: int
    value: int          

class GatsCensusExportByDate(BaseModel):
    date: str
    countryCode: str
    hS10Code: str
    censusUOMId1: int
    censusUOMId2: int
    fasConvertedUOMId: int
    fasNonConvertedUOMId: int
    quantity1: int
    quantity2: int
    value: int  

class GatsCensusReExportByDate(BaseModel):
    date: str
    countryCode: str
    hS10Code: str
    censusUOMId1: int
    censusUOMId2: int
    fasConvertedUOMId: int
    fasNonConvertedUOMId: int
    quantity1: int
    quantity2: int
    value: int      

class GatsCustomDistrictImportByDate(BaseModel):
    customsDistrictId: int
    consumptionQuantity1: int
    consumptionQuantity2: int
    consumptionValue: int
    consumptionCIFValue: int
    cifValue: int
    date: str
    countryCode: str
    hS10Code: str
    censusUOMId1: int
    censusUOMId2: int
    fasConvertedUOMId: int
    fasNonConvertedUOMId: int
    quantity1: int
    quantity2: int
    value: int   

class GatsCustomDistrictExportByDate(BaseModel):
    customsDistrictId: int
    date: str
    countryCode: str
    hS10Code: str
    censusUOMId1: int
    censusUOMId2: int
    fasConvertedUOMId: int
    fasNonConvertedUOMId: int
    quantity1: int
    quantity2: int
    value: int  


class GatsCustomDistrictReExportByDate(BaseModel):
    customsDistrictId: int
    date: str
    countryCode: str
    hS10Code: str
    censusUOMId1: int
    censusUOMId2: int
    fasConvertedUOMId: int
    fasNonConvertedUOMId: int
    quantity1: int
    quantity2: int
    value: int 

class GatsUntradeImportReporterYear(BaseModel):
    year: str
    reporterCountryCode: str
    partnerCountryCode: str
    hS6Cod: str
    censusUOMId1: int
    censusUOMId2: int
    fasConvertedUOMId: int
    fasNonConvertedUOMId: int
    quantity1: int
    quantity2: int
    value: int          
