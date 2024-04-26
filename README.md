# IS3107: Corn Price Forecasting

## Quick Start 🚀 

```
git clone https://github.com/your-username/corn-price-forecasting.git
cd corn-price-forecasting
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
pip install -r requirements.txt
```

## Overview
This project aims to develop an advanced platform tailored for providing accurate forecasts of corn prices, which are crucial for hedgers involved in risk management and speculators seeking profit from market fluctuations. At the heart of this platform is a robust ETLT (Extract, Transform, Load, Transform) pipeline coupled with an analytical dashboard that efficiently processes and analyses data from a variety of sources. This integration facilitates the transformation of complex data sets into accessible, actionable insights presented through user-friendly visualisations.

## Project Structure
The project is organized as follows:

```usda-api/src/app/```: Project jupyter notebooks and main scripts that will be used to spin up the applications\
```usda-api/src/dags/```: Python scripts for airflow pipelines to ingest data sources\
```usda-api/src/data/```: Saved Data that in csv format that has been extracted (For quick reference)\
```usda-api/src/usda-api/scrapers```: Our scrapers used for data extraction in the project\
```requirements.txt```: Python package dependencies.

## Data Sources
The corn price data used in this project is sourced from the U.S. Department of Agriculture (USDA) and includes historical prices, production volumes, and other relevant agricultural economic indicators.

Additional datasets include weather data and commodity market trends, which have been integrated to improve the forecasting accuracy. 

For more information, refer to it in the final report [here](https://docs.google.com/document/d/1FDvBlz3GWmRHNUpMuhGE1cfLAzU__xPoNq6tZdMvuKI/edit)

## License
This project is licensed under the MIT License - see the LICENSE file for details.