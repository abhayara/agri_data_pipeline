with source as (
    select * from {{ ref('seed_production') }}
)

select
    production_id,
    farm_id,
    crop_id,
    date,
    quantity_produced,
    cost
from source 