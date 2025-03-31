with source as (
    select * from {{ ref('seed_harvest') }}
)

select
    harvest_id,
    farm_id,
    crop_id,
    yield_amount,
    current_timestamp() as loaded_at
from source 