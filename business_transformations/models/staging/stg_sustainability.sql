with source as (
    select * from {{ source('agri_data', 'raw_data') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['Farm_ID', 'timestamp']) }} as sustainability_id,
    Farm_ID as farm_id,
    Carbon_Footprint as carbon_footprint,
    Water_Footprint as water_footprint,
    Sustainability_Score as sustainability_score,
    Irrigation_Method as irrigation_method,
    Fertilizer_Type as fertilizer_type,
    Pesticide_Type as pesticide_type,
    Certification as certification,
    timestamp,
    created_at
from source 