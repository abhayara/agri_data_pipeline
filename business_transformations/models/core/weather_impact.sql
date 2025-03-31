with crop_dim as (
    select * from {{ ref('stg_crop') }}
),

yield_facts as (
    select * from {{ ref('stg_yield') }}
),

weather_dim as (
    select * from {{ ref('stg_weather') }}
),

-- Join yields with weather data
yield_weather as (
    select
        y.crop_id,
        y.crop_type,
        y.crop_variety,
        y.farm_id,
        y.planting_date,
        y.harvest_date,
        y.actual_yield,
        y.expected_yield,
        w.temperature,
        w.humidity,
        w.rainfall,
        w.sunlight
    from yield_facts y
    join weather_dim w on y.farm_id = w.farm_id
    where y.harvest_date is not null
),

-- Calculate correlation between weather factors and yield for each crop
temperature_impact as (
    select
        crop_id,
        crop_type,
        crop_variety,
        corr(actual_yield, temperature) as temperature_correlation,
        avg(temperature) as avg_temperature,
        stddev(temperature) as stddev_temperature,
        case
            when abs(corr(actual_yield, temperature)) between 0.7 and 1.0 then 'High'
            when abs(corr(actual_yield, temperature)) between 0.3 and 0.7 then 'Medium'
            else 'Low'
        end as temperature_sensitivity
    from yield_weather
    group by 1, 2, 3
),

precipitation_impact as (
    select
        crop_id,
        crop_type,
        crop_variety,
        corr(actual_yield, rainfall) as rainfall_correlation,
        avg(rainfall) as avg_rainfall,
        stddev(rainfall) as stddev_rainfall,
        case
            when abs(corr(actual_yield, rainfall)) between 0.7 and 1.0 then 'High'
            when abs(corr(actual_yield, rainfall)) between 0.3 and 0.7 then 'Medium'
            else 'Low'
        end as precipitation_sensitivity
    from yield_weather
    group by 1, 2, 3
),

humidity_impact as (
    select
        crop_id,
        crop_type,
        crop_variety,
        corr(actual_yield, humidity) as humidity_correlation,
        avg(humidity) as avg_humidity,
        stddev(humidity) as stddev_humidity,
        case
            when abs(corr(actual_yield, humidity)) between 0.7 and 1.0 then 'High'
            when abs(corr(actual_yield, humidity)) between 0.3 and 0.7 then 'Medium'
            else 'Low'
        end as humidity_sensitivity
    from yield_weather
    group by 1, 2, 3
),

sunlight_impact as (
    select
        crop_id,
        crop_type,
        crop_variety,
        corr(actual_yield, sunlight) as sunlight_correlation,
        avg(sunlight) as avg_sunlight,
        stddev(sunlight) as stddev_sunlight,
        case
            when abs(corr(actual_yield, sunlight)) between 0.7 and 1.0 then 'High'
            when abs(corr(actual_yield, sunlight)) between 0.3 and 0.7 then 'Medium'
            else 'Low'
        end as sunlight_sensitivity
    from yield_weather
    group by 1, 2, 3
)

select
    c.crop_id,
    c.crop_type,
    c.crop_variety as crop_name,
    ti.temperature_correlation,
    ti.avg_temperature,
    ti.temperature_sensitivity,
    pi.rainfall_correlation,
    pi.avg_rainfall,
    pi.precipitation_sensitivity,
    hi.humidity_correlation,
    hi.avg_humidity,
    hi.humidity_sensitivity,
    si.sunlight_correlation,
    si.avg_sunlight,
    si.sunlight_sensitivity,
    case
        when ti.temperature_sensitivity = 'High' or 
             pi.precipitation_sensitivity = 'High' or
             hi.humidity_sensitivity = 'High' or
             si.sunlight_sensitivity = 'High' 
        then 'High'
        when ti.temperature_sensitivity = 'Medium' or 
             pi.precipitation_sensitivity = 'Medium' or
             hi.humidity_sensitivity = 'Medium' or
             si.sunlight_sensitivity = 'Medium'
        then 'Medium'
        else 'Low'
    end as weather_risk_score,
    case
        when ti.temperature_sensitivity = 'High' then 'Consider temperature management strategies. '
        else ''
    end ||
    case
        when pi.precipitation_sensitivity = 'High' then 'Implement water management techniques. '
        else ''
    end ||
    case
        when hi.humidity_sensitivity = 'High' then 'Monitor and manage humidity levels. '
        else ''
    end ||
    case
        when si.sunlight_sensitivity = 'High' then 'Optimize for sunlight exposure. '
        else ''
    end as weather_recommendations,
    current_timestamp() as updated_at
from crop_dim c
left join temperature_impact ti on c.crop_id = ti.crop_id
left join precipitation_impact pi on c.crop_id = pi.crop_id
left join humidity_impact hi on c.crop_id = hi.crop_id
left join sunlight_impact si on c.crop_id = si.crop_id 