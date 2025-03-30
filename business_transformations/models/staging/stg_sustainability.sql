{{ config(
    materialized = 'view'
)}}

SELECT
    sustainability_id,
    farm_id,
    date,
    water_usage,
    carbon_footprint,
    pesticide_usage,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ source('agri_data', 'fact_sustainability') }} 