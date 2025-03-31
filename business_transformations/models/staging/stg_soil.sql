with source as (
    select * from {{ source('agri_data', 'raw_data') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['Farm_ID', 'timestamp']) }} as soil_id,
    Farm_ID as farm_id,
    Soil_Type as soil_type,
    Soil_pH as soil_ph,
    Soil_Moisture as soil_moisture,
    Soil_Temperature as soil_temperature,
    Soil_Fertility as soil_fertility,
    timestamp,
    created_at
from source 