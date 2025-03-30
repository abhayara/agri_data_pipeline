with source as (
    select * from {{ ref('seed_farm') }}
)

select
    farm_id,
    farm_name,
    farm_location as location,
    farm_size as size_hectares,
    established_date
from source 