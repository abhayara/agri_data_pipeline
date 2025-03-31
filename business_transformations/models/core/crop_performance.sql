/*
Crop Performance Data Mart
A comprehensive view of crop performance metrics for analysis
*/

with crop_data as (
    select * from {{ ref('stg_crop') }}
),

full_data as (
    select * from {{ ref('stg_full_data_seed') }}
),

crops as (
    select * from {{ ref('stg_crop') }}
),

harvest as (
    select * from {{ ref('stg_harvest') }}
),

production as (
    select * from {{ ref('stg_production') }}
),

yield_data as (
    select * from {{ ref('stg_yield') }}
)

select
    c.crop_type,
    c.crop_name as crop_variety,
    crops.crop_name,
    crops.growing_season,
    crops.water_needs,
    avg(fd.expected_yield) as avg_expected_yield,
    avg(fd.actual_yield) as avg_actual_yield,
    avg(fd.growing_period_days) as avg_growing_period_days,
    avg(fd.actual_yield) / nullif(avg(fd.expected_yield), 0) * 100 as yield_efficiency_pct,
    
    -- Yield metrics from seed data
    avg(y.yield_per_hectare) as avg_yield_per_hectare,
    
    -- Production metrics from seed data
    avg(p.quantity_produced) as avg_quantity_produced,
    avg(p.cost) as avg_production_cost,
    avg(p.quantity_produced / nullif(p.cost, 0)) as production_efficiency,
    
    -- Harvest metrics from seed data
    avg(h.yield_amount) as avg_harvest_yield,
    
    -- Additional metrics from full data
    count(distinct fd.farm_id) as farm_count,
    avg(fd.sustainability_score) as avg_sustainability_score,
    
    -- Time-based aggregations
    avg(case when fd.year = extract(year from current_date()) 
        then fd.actual_yield else null end) as current_year_yield,
    avg(case when fd.year = extract(year from current_date()) - 1
        then fd.actual_yield else null end) as previous_year_yield,
    
    -- Year-over-year growth
    (avg(case when fd.year = extract(year from current_date()) then fd.actual_yield else null end) / 
     nullif(avg(case when fd.year = extract(year from current_date()) - 1 then fd.actual_yield else null end), 0) - 1) * 100 as yoy_growth_pct,
    
    -- Seasonal performance
    avg(case when fd.month between 3 and 5 then fd.actual_yield else null end) as spring_yield,
    avg(case when fd.month between 6 and 8 then fd.actual_yield else null end) as summer_yield,
    avg(case when fd.month between 9 and 11 then fd.actual_yield else null end) as fall_yield,
    avg(case when fd.month in (12, 1, 2) then fd.actual_yield else null end) as winter_yield,
    
    -- Success indicators
    avg(fd.yield_percentage) as avg_yield_percentage,
    
    -- Composite score (weighted average of multiple performance factors)
    (avg(fd.yield_percentage) * 0.5) +
    (avg(p.quantity_produced / nullif(p.cost, 0)) / 10 * 0.3) +
    (avg(y.yield_per_hectare) / 5 * 0.2) as crop_performance_score,
    
    -- Rankings
    row_number() over(order by avg(fd.yield_percentage) desc) as yield_efficiency_rank,
    
    current_timestamp() as generated_at
from 
    crop_data c
left join 
    full_data fd on c.crop_type = fd.crop_type
left join
    crops on c.crop_id = crops.crop_id
left join
    yield_data y on crops.crop_id = y.crop_id
left join
    production p on crops.crop_id = p.crop_id
left join
    harvest h on crops.crop_id = h.crop_id
group by 
    c.crop_type,
    c.crop_name,
    crops.crop_name,
    crops.growing_season,
    crops.water_needs 