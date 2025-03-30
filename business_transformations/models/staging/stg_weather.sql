with source as (
    select * from {{ ref('seed_weather') }}
)

select
    weather_id,
    date,
    location,
    temperature,
    precipitation,
    humidity
from source 