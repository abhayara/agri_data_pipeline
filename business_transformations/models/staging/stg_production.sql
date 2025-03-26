{{ config(
    materialized = 'view'
)}}

SELECT
    production_id,
    farm_id,
    crop_id,
    date,
    quantity_produced,
    cost,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ source('agri_data', 'fact_production') }} 