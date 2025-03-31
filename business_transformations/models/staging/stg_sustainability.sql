with source as (
    select * from {{ ref('seed_sustainability') }}
)

select
    sustainability_id,
    farm_id,
    water_usage,
    carbon_footprint,
    pesticide_usage,
    current_timestamp() as loaded_at
from source 