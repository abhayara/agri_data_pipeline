{{ config(
    materialized = 'view'
)}}

SELECT
    weather_id,
    date,
    location,
    temperature,
    precipitation,
    humidity,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ source('agri_data', 'dim_weather') }} 