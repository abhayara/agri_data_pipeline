with source as (
    select * from {{ source('agri_data', 'raw_data') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['Farm_ID', 'Crop_ID', 'Harvest_Date']) }} as yield_id,
    Farm_ID as farm_id,
    Crop_ID as crop_id,
    Crop_Type as crop_type,
    Crop_Variety as crop_variety,
    Planting_Date as planting_date,
    Harvest_Date as harvest_date,
    Expected_Yield as expected_yield,
    Actual_Yield as actual_yield,
    Yield_Unit as yield_unit,
    (Actual_Yield/Expected_Yield) as yield_ratio,
    created_at
from source
where Harvest_Date is not null 