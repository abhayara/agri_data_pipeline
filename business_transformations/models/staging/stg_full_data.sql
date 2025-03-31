with source as (
    select * from {{ source('agri_data', 'full_data') }}
)

select
    Farm_ID as farm_id,
    Crop_ID as crop_id,
    Farm_Type as farm_type,
    Farm_Size_Acres as farm_size_acres,
    Planting_Date as planting_date,
    Harvest_Date as harvest_date,
    Expected_Yield as expected_yield,
    Actual_Yield as actual_yield,
    Soil_Moisture as soil_moisture,
    Soil_Temperature as soil_temperature,
    Sustainability_Score as sustainability_score,
    year,
    month,
    date(extract(year from Planting_Date), extract(month from Planting_Date), 1) as planting_month,
    date_diff(Harvest_Date, Planting_Date, DAY) as growing_period_days,
    (Actual_Yield / nullif(Expected_Yield, 0)) * 100 as yield_percentage,
    current_timestamp() as loaded_at
from source 