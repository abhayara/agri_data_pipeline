with source as (
    select * from {{ ref('seed_sustainability') }}
)

select
    sustainability_id,
    farm_id,
    date,
    water_usage,
    carbon_footprint,
    pesticide_usage
from source 