{{ config(
    materialized = 'view'
)}}

SELECT
    harvest_id,
    farm_id,
    crop_id,
    harvest_date,
    yield_amount,
    quality_grade,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ source('agri_data', 'dim_harvest') }} 