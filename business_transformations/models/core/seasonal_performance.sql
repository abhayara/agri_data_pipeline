/*
Seasonal Performance Data Mart
Analysis of crop and farm performance across different seasons and years
*/

with full_data as (
    select * from {{ ref('stg_full_data_seed') }}
),

farms as (
    select * from {{ ref('stg_farm') }}
),

crops as (
    select * from {{ ref('stg_crop') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

-- Create a base aggregation with season calculation
base_metrics as (
    select
        fd.farm_id,
        farms.farm_name,
        farms.farm_location,
        fd.crop_id,
        crops.crop_name,
        crops.crop_type,
        fd.year,
        
        -- Define season based on month
        case
            when fd.month between 3 and 5 then 'Spring'
            when fd.month between 6 and 8 then 'Summer'
            when fd.month between 9 and 11 then 'Fall'
            else 'Winter'
        end as season,
        
        -- Seasonal metrics
        avg(fd.actual_yield) as avg_yield,
        avg(fd.expected_yield) as avg_expected_yield,
        avg(fd.yield_percentage) as avg_yield_percentage,
        avg(fd.sustainability_score) as avg_sustainability_score,
        avg(fd.growing_period_days) as avg_growing_period,
        
        -- Weather impact analysis (using sample weather data)
        avg(w.temperature) as avg_temperature,
        avg(w.precipitation) as avg_precipitation,
        avg(w.humidity) as avg_humidity
    from
        full_data fd
    left join
        farms on fd.farm_id = farms.farm_id
    left join
        crops on cast(fd.crop_id as int64) = crops.crop_id
    left join
        weather w on 1=1 -- Weather is just sample data for now
    group by
        fd.farm_id,
        farms.farm_name,
        farms.farm_location,
        fd.crop_id,
        crops.crop_name,
        crops.crop_type,
        fd.year,
        season
)

-- Final query with window function
select
    farm_id,
    farm_name,
    farm_location,
    crop_id,
    crop_name,
    crop_type,
    year,
    season,
    avg_yield,
    avg_expected_yield,
    avg_yield_percentage,
    avg_sustainability_score,
    avg_growing_period,
    
    -- Now the lag function can reference the season column directly
    avg_yield - lag(avg_yield, 1) over (
        partition by farm_id, crop_id, season
        order by year
    ) as yield_change_from_previous_year,
    
    avg_temperature,
    avg_precipitation,
    avg_humidity,
    
    -- Season-specific recommendations based on performance
    case
        when avg_yield_percentage < 80 and season = 'Summer' 
            then 'Consider additional irrigation or heat-resistant varieties'
        when avg_yield_percentage < 80 and season = 'Winter'
            then 'Consider cold protection measures or winter-hardy varieties'
        when avg_yield_percentage < 80 then 'Review growing conditions and varieties'
        else 'Current practices are effective'
    end as season_recommendations,
    
    -- Simple season rating (1-5)
    case
        when avg_yield_percentage >= 95 then 5
        when avg_yield_percentage >= 85 then 4
        when avg_yield_percentage >= 75 then 3
        when avg_yield_percentage >= 65 then 2
        else 1
    end as season_performance_rating,
    
    current_timestamp() as generated_at
from
    base_metrics 