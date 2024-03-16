from pydantic import BaseModel
from datetime import datetime

class PSDCountriesType(BaseModel):
    countryCode: str
    countryName: str
    regionCode: str
    gencCode: str
class PSDCountriesCleanedType(BaseModel):
    country_code: str
    country_name: str
    region_code: str
    genc_code: str
  
  
class PSDCommoditiesType(BaseModel):
    commodityCode: str
    commodityName: str
class PSDCommoditiesCleanedType(BaseModel):
    commodity_code: str
    commodity_name: str
    
    
class PSDUnitsOfMeasureType(BaseModel):
    unitId: int
    unitDescription: str
class PSDUnitsOfMeasureCleanedType(BaseModel):
    unit_id: int
    unit_description: str
   
    
class PSDCommoditiesAttributesType(BaseModel):
    attributeId: int
    attibuteName: str
class PSDCommoditiesAttributesCleanedType(BaseModel):
    attribute_id: int
    attibute_name: str
    
    
class PSDCountryCommoditiesForecastType(BaseModel):
    commodityCode : str
    countryCode : str
    marketYear : str
    calenderYear : str
    month : str
    attributeId : int
    unitId : int
    value : int
class PSDCountryCommoditiesForecastCleanedType(BaseModel):
    commodity_code : str
    country_code : str
    market_year : str
    calender_year : str
    month : str
    attribute_id : int
    unit_id : int
    value : int
