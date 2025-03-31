with source as (
    select * from {{ source('agri_data', 'raw_data') }}
)

select
    Farm_ID as farm_id,
    Farm_Type as farm_type,
    Farm_Size_Acres as farm_size_acres,
    Farm_Location as farm_location,
    Farmer_ID as farmer_id,
    Farmer_Name as farmer_name,
    Farmer_Contact as farmer_contact,
    created_at
from source 