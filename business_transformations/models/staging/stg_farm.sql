with source as (
    select * from {{ ref('seed_farm') }}
)

select
    farm_id,
    farm_name,
    farm_location,
    farm_size,
    established_date,
    current_timestamp() as loaded_at
from source 