with source as (
    select * from {{ source('agri_data', 'raw_data') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['Farm_ID', 'timestamp']) }} as weather_id,
    Farm_ID as farm_id,
    Weather_Temperature as temperature,
    Weather_Humidity as humidity,
    Weather_Rainfall as rainfall, 
    Weather_Sunlight as sunlight,
    timestamp,
    created_at
from source 