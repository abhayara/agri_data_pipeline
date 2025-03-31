with source as (
    select * from {{ ref('seed_soil') }}
)

select
    soil_id,
    farm_id,
    ph_level,
    organic_matter,
    current_timestamp() as loaded_at
from source 