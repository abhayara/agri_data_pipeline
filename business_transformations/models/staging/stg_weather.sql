with source as (
    select * from {{ ref('seed_weather') }}
)

select
    weather_id,
    temperature,
    precipitation,
    humidity,
    current_timestamp() as loaded_at
from source 