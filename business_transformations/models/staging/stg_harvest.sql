with source as (
    select * from {{ ref('seed_harvest') }}
)

select
    harvest_id,
    farm_id,
    crop_id,
    harvest_date,
    yield_amount,
    quality_grade
from source 