{{ config(
    materialized = 'view'
)}}

SELECT
    yield_id,
    farm_id,
    crop_id,
    harvest_id,
    yield_per_hectare,
    year,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ source('agri_data', 'fact_yield') }} 