with source as (
    select * from {{ source('agri_data', 'farm_analysis') }}
)

select
    Farm_ID as farm_id,
    Farm_Type as farm_type,
    avg_Farm_Size_Acres as avg_farm_size_acres,
    avg_Soil_Moisture as avg_soil_moisture,
    avg_Soil_Temperature as avg_soil_temperature,
    current_timestamp() as loaded_at
from source 