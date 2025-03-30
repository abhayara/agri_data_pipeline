with source as (
    select * from {{ ref('seed_farm') }}
)

select
    farm_id,
    farm_name,
    location,
    size_hectares,
    established_date
from source 