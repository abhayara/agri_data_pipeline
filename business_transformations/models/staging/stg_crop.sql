{{ config(
    materialized = 'view'
)}}

SELECT
    crop_id,
    crop_name,
    crop_type,
    growing_season,
    water_needs,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ source('agri_data', 'dim_crop') }} 