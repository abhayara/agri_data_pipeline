with source as (
    select * from {{ ref('seed_yield') }}
)

select
    yield_id,
    farm_id,
    crop_id,
    harvest_id,
    yield_per_hectare,
    year,
    current_timestamp() as loaded_at
from source 