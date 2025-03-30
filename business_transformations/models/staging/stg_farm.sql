{{ config(
    materialized = 'view'
)}}

SELECT
    farm_id,
    farm_name,
    location,
    size_hectares,
    established_date,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ source('agri_data', 'dim_farm') }} 