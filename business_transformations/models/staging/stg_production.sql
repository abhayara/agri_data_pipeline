with source as (
    select * from {{ source('agri_data', 'raw_data') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['Farm_ID', 'Crop_ID', 'timestamp']) }} as production_id,
    Farm_ID as farm_id,
    Crop_ID as crop_id,
    Production_Cost as production_cost,
    Market_Price as market_price,
    Total_Revenue as total_revenue,
    Profit_Margin as profit_margin,
    Irrigation_Method as irrigation_method,
    Irrigation_Amount as irrigation_amount,
    Fertilizer_Type as fertilizer_type,
    Fertilizer_Amount as fertilizer_amount,
    Pesticide_Type as pesticide_type,
    Pesticide_Amount as pesticide_amount,
    Equipment_Used as equipment_used,
    Labor_Hours as labor_hours,
    timestamp,
    created_at
from source 