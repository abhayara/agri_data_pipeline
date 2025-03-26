{{ config(
    materialized = 'view'
)}}

SELECT
    soil_id,
    farm_id,
    ph_level,
    nutrient_content,
    texture,
    organic_matter,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ source('agri_data', 'dim_soil') }} 