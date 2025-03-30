with source as (
    select * from {{ ref('seed_soil') }}
)

select
    soil_id,
    farm_id,
    ph_level,
    nutrient_content,
    texture,
    organic_matter
from source 