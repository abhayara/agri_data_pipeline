with farm_dim as (
    select * from {{ ref('stg_farm') }}
),

crop_dim as (
    select * from {{ ref('stg_crop') }}
),

yield_facts as (
    select * from {{ ref('stg_yield') }}
),

soil_dim as (
    select * from {{ ref('stg_soil') }}
),

weather_dim as (
    select * from {{ ref('stg_weather') }}
),

-- Calculate current yield metrics
current_yields as (
    select
        y.farm_id,
        y.crop_id,
        c.crop_type,
        c.crop_variety,
        avg(y.actual_yield) as current_yield,
        y.yield_unit
    from yield_facts y
    join crop_dim c on y.crop_id = c.crop_id
    group by 1, 2, 3, 4, 6
),

-- Find optimal yields (top 10% performers for each crop type and variety)
optimal_yields as (
    select
        crop_type,
        crop_variety,
        percentile_cont(0.9) over (partition by crop_type, crop_variety) as optimal_yield
    from current_yields
),

-- Join current yields with optimal yields to calculate gaps
yield_gaps as (
    select
        cy.farm_id,
        cy.crop_id,
        cy.crop_type,
        cy.crop_variety,
        cy.current_yield,
        oy.optimal_yield,
        (oy.optimal_yield - cy.current_yield) as yield_gap,
        case
            when cy.current_yield > 0
            then (oy.optimal_yield - cy.current_yield) / cy.current_yield * 100
            else 0
        end as yield_gap_percentage,
        cy.yield_unit
    from current_yields cy
    join optimal_yields oy on cy.crop_type = oy.crop_type and cy.crop_variety = oy.crop_variety
),

-- Average soil conditions for each farm
farm_soil as (
    select
        farm_id,
        avg(soil_ph) as avg_soil_ph,
        avg(soil_moisture) as avg_soil_moisture,
        avg(soil_temperature) as avg_soil_temperature,
        avg(soil_fertility) as avg_soil_fertility,
        mode() within group (order by soil_type) as common_soil_type
    from soil_dim
    group by 1
),

-- Average weather conditions for each farm
farm_weather as (
    select
        farm_id,
        avg(temperature) as avg_temperature,
        avg(humidity) as avg_humidity,
        avg(rainfall) as avg_rainfall,
        avg(sunlight) as avg_sunlight
    from weather_dim
    group by 1
)

select
    yg.farm_id,
    f.farm_name,
    yg.crop_id,
    yg.crop_type,
    yg.crop_variety as crop_name,
    yg.current_yield,
    yg.optimal_yield,
    yg.yield_gap,
    yg.yield_gap_percentage,
    yg.yield_unit,
    fs.avg_soil_ph,
    fs.avg_soil_moisture,
    fs.avg_soil_temperature,
    fs.avg_soil_fertility,
    fs.common_soil_type,
    fw.avg_temperature,
    fw.avg_humidity,
    fw.avg_rainfall,
    fw.avg_sunlight,
    case
        when yg.yield_gap_percentage > 50 then 'High Improvement Potential'
        when yg.yield_gap_percentage between 20 and 50 then 'Medium Improvement Potential'
        when yg.yield_gap_percentage between 5 and 20 then 'Small Improvement Potential'
        else 'Optimal Production'
    end as optimization_category,
    case
        when fs.avg_soil_ph < 6.0 then 'Consider soil pH adjustment. '
        else ''
    end || 
    case
        when fs.avg_soil_fertility < 5 then 'Improve soil fertility with appropriate amendments. '
        else ''
    end ||
    case
        when fw.avg_rainfall < 50 then 'Consider improved irrigation strategies. '
        else ''
    end ||
    case
        when yg.yield_gap_percentage > 30 then 'Evaluate seed varieties and farming practices. '
        else ''
    end as optimization_recommendations,
    current_timestamp() as updated_at
from yield_gaps yg
join farm_dim f on yg.farm_id = f.farm_id
left join farm_soil fs on yg.farm_id = fs.farm_id
left join farm_weather fw on yg.farm_id = fw.farm_id 