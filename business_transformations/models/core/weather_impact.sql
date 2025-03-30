with weather_data as (
    select * from {{ ref('stg_weather') }}
),

farm_data as (
    select * from {{ ref('stg_farm') }}
),

production_data as (
    select * from {{ ref('stg_production') }}
),

yield_data as (
    select * from {{ ref('stg_yield') }}
),

weather_production_correlation as (
    select
        w.location,
        avg(w.temperature) as avg_temperature,
        avg(w.precipitation) as avg_precipitation,
        avg(w.humidity) as avg_humidity,
        avg(p.quantity_produced) as avg_production
    from weather_data w
    join farm_data f on w.location = f.location
    join production_data p on f.farm_id = p.farm_id
    where w.date = p.date
    group by 1
),

temperature_impact as (
    select
        w.location,
        corr(w.temperature, y.yield_per_hectare) as temperature_yield_correlation
    from weather_data w
    join farm_data f on w.location = f.location
    join yield_data y on f.farm_id = y.farm_id
    group by 1
),

precipitation_impact as (
    select
        w.location,
        corr(w.precipitation, y.yield_per_hectare) as precipitation_yield_correlation
    from weather_data w
    join farm_data f on w.location = f.location
    join yield_data y on f.farm_id = y.farm_id
    group by 1
),

humidity_impact as (
    select
        w.location,
        corr(w.humidity, y.yield_per_hectare) as humidity_yield_correlation
    from weather_data w
    join farm_data f on w.location = f.location
    join yield_data y on f.farm_id = y.farm_id
    group by 1
)

select
    wpc.location,
    wpc.avg_temperature,
    wpc.avg_precipitation,
    wpc.avg_humidity,
    wpc.avg_production,
    ti.temperature_yield_correlation,
    pi.precipitation_yield_correlation,
    hi.humidity_yield_correlation,
    case
        when abs(ti.temperature_yield_correlation) > abs(pi.precipitation_yield_correlation) 
             and abs(ti.temperature_yield_correlation) > abs(hi.humidity_yield_correlation)
        then 'Temperature'
        when abs(pi.precipitation_yield_correlation) > abs(ti.temperature_yield_correlation)
             and abs(pi.precipitation_yield_correlation) > abs(hi.humidity_yield_correlation)
        then 'Precipitation'
        else 'Humidity'
    end as most_influential_factor
from weather_production_correlation wpc
join temperature_impact ti on wpc.location = ti.location
join precipitation_impact pi on wpc.location = pi.location
join humidity_impact hi on wpc.location = hi.location 