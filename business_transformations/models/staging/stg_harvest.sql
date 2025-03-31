with source as (
    select * from {{ source('agri_data', 'raw_data') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['Farm_ID', 'Crop_ID', 'Harvest_Date']) }} as harvest_id,
    Farm_ID as farm_id,
    Crop_ID as crop_id,
    Harvest_Date as harvest_date,
    Actual_Yield as actual_yield,
    Yield_Unit as yield_unit,
    Quality_Grade as quality_grade,
    Certification as certification,
    created_at
from source
where Harvest_Date is not null 