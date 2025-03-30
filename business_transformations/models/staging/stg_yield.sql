with source as (
    select * from {{ ref('seed_yield') }}
)

select
    yield_id,
    farm_id,
    crop_id,
    harvest_id,
    yield_per_hectare,
    year
from source 