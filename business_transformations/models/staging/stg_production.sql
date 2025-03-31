with source as (
    select * from {{ ref('seed_production') }}
)

select
    production_id,
    farm_id,
    crop_id,
    quantity_produced,
    cost,
    current_timestamp() as loaded_at
from source 