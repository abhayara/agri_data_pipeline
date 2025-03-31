/*
Sustainability Metrics Data Mart
A comprehensive view of sustainability metrics for analysis and reporting
*/

with farm_data as (
    select * from {{ ref('stg_farm') }}
),

crop_data as (
    select * from {{ ref('stg_crop') }}
),

full_data as (
    select * from {{ ref('stg_full_data_seed') }}
),

farms as (
    select * from {{ ref('stg_farm') }}
),

crops as (
    select * from {{ ref('stg_crop') }}
),

sustainability as (
    select * from {{ ref('stg_sustainability') }}
),

soil_data as (
    select * from {{ ref('stg_soil') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
)

select
    fd.farm_id,
    farms.farm_name,
    farms.farm_location,
    fd.crop_type as farm_type,
    fd.crop_id,
    crops.crop_name,
    crops.crop_type,
    crops.water_needs,
    
    -- Time dimensions
    fd.year,
    fd.month,
    date(extract(year from fd.planting_date), extract(month from fd.planting_date), 1) as planting_month,
    
    -- OLAP sustainability metrics
    fd.sustainability_score,
    
    -- Seed data sustainability metrics
    sus.water_usage,
    sus.carbon_footprint,
    sus.pesticide_usage,
    
    -- Soil metrics
    s.ph_level,
    s.organic_matter,
    
    -- Weather impacts
    avg(w.precipitation) as avg_precipitation,
    avg(w.humidity) as avg_humidity,
    
    -- Comprehensive sustainability score (combined from multiple sources)
    (coalesce(fd.sustainability_score, 0) * 0.4) + 
    (100 - coalesce(sus.water_usage, 0)) * 0.2 + 
    (100 - coalesce(sus.carbon_footprint, 0)) * 0.2 + 
    (100 - coalesce(sus.pesticide_usage, 0)) * 0.2 as comprehensive_sustainability_score,
    
    -- Calculate sustainability tier (1-5 scale)
    case 
        when fd.sustainability_score >= 80 then 5
        when fd.sustainability_score >= 60 then 4
        when fd.sustainability_score >= 40 then 3
        when fd.sustainability_score >= 20 then 2
        else 1
    end as sustainability_tier,
    
    -- Combine with farm averages
    avg(fd.soil_moisture) as avg_soil_moisture,
    avg(fd.soil_temperature) as avg_soil_temperature,
    fd.farm_size_acres,
    
    -- Resource efficiency metrics
    case 
        when crops.water_needs = 'Low' then 1
        when crops.water_needs = 'Medium' then 2
        when crops.water_needs = 'High' then 3
        when crops.water_needs = 'Very High' then 4
        else 0
    end as water_need_rating,
    
    -- Water efficiency (inverse relationship between water usage and yield)
    fd.actual_yield / nullif(sus.water_usage, 0) as water_efficiency,
    
    -- Carbon efficiency
    fd.actual_yield / nullif(sus.carbon_footprint, 0) as carbon_efficiency,
    
    -- Efficiency metrics related to sustainability
    fd.actual_yield / nullif(fd.expected_yield, 0) * 100 as resource_efficiency,
    fd.actual_yield / nullif(fd.farm_size_acres, 0) as yield_density,
    fd.growing_period_days as growing_period,
    
    -- Calculate a normalized sustainability score per farm size
    fd.sustainability_score / nullif(sqrt(fd.farm_size_acres), 0) as size_normalized_score,
    
    -- Calculate sustainability score per yield unit
    fd.sustainability_score / nullif(fd.actual_yield, 0) as yield_normalized_score,
    
    current_timestamp() as generated_at
from 
    full_data fd
left join 
    farm_data fa on fd.farm_id = fa.farm_id
left join
    farms on fd.farm_id = farms.farm_id
left join
    sustainability sus on fd.farm_id = sus.farm_id
left join
    soil_data s on fd.farm_id = s.farm_id
left join
    crops on cast(fd.crop_id as int64) = crops.crop_id
left join
    weather w on 1=1 -- Weather data is just a sample lookup for now
where 
    fd.sustainability_score is not null
group by
    fd.farm_id,
    farms.farm_name,
    farms.farm_location,
    fd.crop_type,
    fd.crop_id,
    crops.crop_name,
    crops.crop_type,
    crops.water_needs,
    fd.year,
    fd.month,
    fd.planting_date,
    fd.sustainability_score,
    sus.water_usage,
    sus.carbon_footprint,
    sus.pesticide_usage,
    s.ph_level,
    s.organic_matter,
    fd.farm_size_acres,
    fd.actual_yield,
    fd.expected_yield,
    fd.growing_period_days 