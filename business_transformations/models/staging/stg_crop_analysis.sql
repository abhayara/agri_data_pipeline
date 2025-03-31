with source as (
    select * from {{ source('agri_data', 'crop_analysis') }}
)

select
    Crop_Type as crop_type,
    Crop_Variety as crop_variety,
    avg_Expected_Yield as avg_expected_yield,
    avg_Actual_Yield as avg_actual_yield,
    avg_Growing_Period_Days as avg_growing_period_days,
    current_timestamp() as loaded_at
from source 