with source as (
    select * from {{ source('agri_data', 'raw_data') }}
)

select
    Crop_ID as crop_id,
    Crop_Type as crop_type,
    Crop_Variety as crop_variety,
    Planting_Date as planting_date,
    Harvest_Date as harvest_date,
    Expected_Yield as expected_yield,
    Actual_Yield as actual_yield,
    Yield_Unit as yield_unit,
    Farm_ID as farm_id,
    created_at
from source 