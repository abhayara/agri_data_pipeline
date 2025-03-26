# Agri Data Business Transformations

This dbt (Data Build Tool) project handles business transformations for the agricultural data pipeline. It transforms the raw data loaded into BigQuery into analytics-ready models.

## Project Structure

- `models/`: Contains SQL transformation models
  - `staging/`: Initial transformations that clean and prepare source data
  - `core/`: Business-level transformations that power analytics
- `macros/`: Reusable SQL snippets and functions
- `analyses/`: Ad-hoc analytical queries
- `tests/`: Data tests to ensure data quality
- `seeds/`: Static data files
- `snapshots/`: Point-in-time snapshots of data

## Data Flow

1. Raw agricultural data is collected from various sources through streaming pipeline
2. Data is processed by the batch pipeline and loaded into BigQuery tables
3. DBT models transform this data into analytical models in the following layers:
   - **Staging models**: Clean and standardize source data
   - **Core models**: Business-level transformations for analytics

## Getting Started

### Prerequisites

- Access to the BigQuery project
- dbt CLI installed (`pip install dbt-bigquery`)
- Google Cloud credentials

### Setup

1. Clone this repository
2. Navigate to the `business_transformations` directory
3. Configure your BigQuery credentials in `profiles.yml`

### Running dbt

```bash
# Navigate to the business_transformations directory
cd business_transformations

# Run all models
dbt run

# Run specific models
dbt run --select staging.dim_farm
dbt run --select core.farm_analytics

# Test your data
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

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