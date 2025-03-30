# Agri Data Business Transformations

This dbt (Data Build Tool) project handles business transformations for the agricultural data pipeline. It transforms the raw data loaded into BigQuery into analytics-ready models.

## Project Structure



## Data Flow



## Getting Started

### Prerequisites

- Access to the BigQuery project
- dbt CLI installed (`pip install dbt-bigquery`)
- Google Cloud credentials

### Setup


### Running dbt



## Available Models

### Staging Models

- `dim_farm`: Farm dimension data
- `dim_crop`: Crop dimension data
- `dim_weather`: Weather dimension data
- `dim_soil`: Soil dimension data
- `dim_harvest`: Harvest dimension data
- `fact_production`: Production facts
- `fact_yield`: Yield facts
- `fact_sustainability`: Sustainability facts

### Core Models

- `farm_analytics`: Farm-level analytics and KPIs
- `crop_performance`: Crop performance metrics
- `sustainability_metrics`: Environmental sustainability metrics
- `yield_optimization`: Yield optimization analysis
- `weather_impact`: Weather impact on crop performance

## Deployment

This dbt project is executed as part of the overall Agri Data Pipeline. The `commands.sh` script includes commands to run the dbt pipeline after data is loaded into BigQuery. 