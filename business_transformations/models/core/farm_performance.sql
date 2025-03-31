/*
Farm Performance Data Mart
A comprehensive view of farm performance metrics for analysis
*/

with farm_analysis as (
    select * from {{ ref('stg_farm') }}
),

full_data as (
    select * from {{ ref('stg_full_data_seed') }}
),

farms as (
    select * from {{ ref('stg_farm') }}
),

soil_data as (
    select * from {{ ref('stg_soil') }}
),

sustainability as (
    select * from {{ ref('stg_sustainability') }}
),

yield_data as (
    select * from {{ ref('stg_yield') }}
)

select
    f.farm_id,
    farms.farm_name,
    farms.farm_location,
    fd.crop_type as farm_type,
    farms.farm_size as avg_farm_size_acres,
    avg(fd.soil_moisture) as avg_soil_moisture,
    avg(fd.soil_temperature) as avg_soil_temperature,
    
    -- Soil quality metrics (from seed data)
    avg(s.ph_level) as avg_soil_ph,
    avg(s.organic_matter) as avg_organic_matter,
    
    -- Sustainability metrics (from seed data)
    avg(sus.water_usage) as avg_water_usage,
    avg(sus.carbon_footprint) as avg_carbon_footprint,
    avg(sus.pesticide_usage) as avg_pesticide_usage,
    
    -- Yield metrics (from seed data combined with OLAP data)
    avg(y.yield_per_hectare) as avg_yield_per_hectare,
    
    -- Aggregations from full data
    avg(fd.actual_yield) as avg_actual_yield,
    avg(fd.expected_yield) as avg_expected_yield,
    sum(fd.actual_yield) as total_actual_yield,
    avg(fd.yield_percentage) as avg_yield_percentage,
    avg(fd.sustainability_score) as avg_sustainability_score,
    
    -- Time-based metrics
    avg(case when fd.year >= extract(year from current_date()) - 1 
        then fd.actual_yield else null end) as recent_avg_yield,
    
    -- Efficiency metrics
    avg(fd.growing_period_days) as avg_growing_period,
    avg(fd.actual_yield / nullif(fd.farm_size_acres, 0)) as yield_per_acre,
    
    -- Farm performance score (composite metric)
    (avg(fd.yield_percentage) * 0.4) + 
    (avg(fd.sustainability_score) * 0.3) + 
    (100 - avg(coalesce(sus.water_usage, 0)) * 0.1) + 
    (100 - avg(coalesce(sus.carbon_footprint, 0)) * 0.2) as farm_performance_score,
    
    -- Relative rankings
    row_number() over(order by avg(fd.yield_percentage) desc) as yield_rank,
    row_number() over(order by avg(fd.sustainability_score) desc) as sustainability_rank,
    row_number() over(partition by fd.crop_type order by avg(fd.actual_yield) desc) as yield_rank_in_type,
    
    current_timestamp() as generated_at
from 
    farm_analysis f
left join 
    full_data fd on f.farm_id = fd.farm_id
left join
    farms on f.farm_id = farms.farm_id
left join
    soil_data s on f.farm_id = s.farm_id
left join
    sustainability sus on f.farm_id = sus.farm_id
left join
    yield_data y on f.farm_id = y.farm_id
group by 
    f.farm_id, 
    farms.farm_name,
    farms.farm_location,
    fd.crop_type, 
    farms.farm_size 