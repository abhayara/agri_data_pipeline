{{
    config(
        materialized='view'
    )
}}

-- Create a synthetic full_data model from seed data
with farm_data as (
    select 
        farm_id,
        farm_name,
        farm_location,
        farm_size as farm_size_acres,
        extract(year from established_date) as established_year
    from {{ ref('seed_farm') }}
),

crop_data as (
    select 
        crop_id,
        crop_name,
        crop_type,
        growing_season,
        water_needs
    from {{ ref('seed_crop') }}
),

harvest_data as (
    select 
        harvest_id,
        farm_id,
        crop_id,
        yield_amount
    from {{ ref('seed_harvest') }}
),

yield_data as (
    select 
        yield_id,
        farm_id,
        crop_id,
        harvest_id,
        yield_per_hectare,
        year
    from {{ ref('seed_yield') }}
),

sustainability_data as (
    select 
        sustainability_id,
        farm_id,
        water_usage,
        carbon_footprint,
        pesticide_usage
    from {{ ref('seed_sustainability') }}
),

soil_data as (
    select 
        soil_id,
        farm_id,
        ph_level,
        organic_matter
    from {{ ref('seed_soil') }}
),

-- Generate planting and harvest dates based on year and growing season
synthetic_dates as (
    select 
        y.farm_id,
        y.crop_id,
        y.year,
        case 
            when c.growing_season = 'Spring' then date(cast(y.year as string) || '-03-15')
            when c.growing_season = 'Summer' then date(cast(y.year as string) || '-06-15')
            when c.growing_season = 'Winter' then date(cast(y.year as string) || '-11-15')
            when c.growing_season = 'Annual' then date(cast(y.year as string) || '-04-01')
            else date(cast(y.year as string) || '-05-01')
        end as planting_date,
        case 
            when c.growing_season = 'Spring' then date(cast(y.year as string) || '-06-15')
            when c.growing_season = 'Summer' then date(cast(y.year as string) || '-09-15')
            when c.growing_season = 'Winter' then date(cast(y.year as string) || '-02-15')
            when c.growing_season = 'Annual' then date(cast(y.year as string) || '-10-01')
            else date(cast(y.year as string) || '-09-01')
        end as harvest_date
    from yield_data y
    join crop_data c on y.crop_id = c.crop_id
)

select
    f.farm_id as farm_id,
    cast(c.crop_id as string) as crop_id,
    f.farm_name as farmer_contact,
    f.farm_location as farm_location,
    f.established_year as established_year,
    c.crop_type as crop_type,
    c.crop_name as crop_name,
    f.farm_size_acres as farm_size_acres,
    d.planting_date as planting_date,
    d.harvest_date as harvest_date,
    h.yield_amount as expected_yield,
    case 
        when h.yield_amount > 0 then 
            h.yield_amount * (0.5 + rand() * 0.7) -- Random between 50-120% of expected
        else 100
    end as actual_yield,
    coalesce(s.ph_level * 10, 60) as soil_moisture,
    coalesce(s.ph_level * 5 + 40, 55) as soil_temperature,
    coalesce(100 - sus.carbon_footprint, 75) as sustainability_score,
    extract(year from d.planting_date) as year,
    extract(month from d.planting_date) as month,
    'kg/hectare' as yield_unit,
    coalesce(s.ph_level, 6.5) as soil_ph,
    case 
        when s.organic_matter > 3 then 'High'
        when s.organic_matter > 1.5 then 'Medium'
        else 'Low'
    end as soil_fertility,
    8 as weather_sunlight,
    coalesce(sus.water_usage * 10, 200) as irrigation_amount,
    100 as fertilizer_amount,
    case 
        when sus.pesticide_usage > 50 then 'Chemical'
        else 'Organic'
    end as pesticide_type,
    coalesce(sus.pesticide_usage, 30) as pesticide_amount,
    case 
        when f.farm_size_acres > 200 then 'Heavy Machinery'
        when f.farm_size_acres > 100 then 'Tractor'
        else 'Manual Tools'
    end as equipment_used,
    f.farm_size_acres * 2 as labor_hours,
    h.yield_amount * 10 as total_revenue,
    case 
        when coalesce(100 - sus.carbon_footprint, 75) > 80 then 'A'
        when coalesce(100 - sus.carbon_footprint, 75) > 60 then 'B'
        when coalesce(100 - sus.carbon_footprint, 75) > 40 then 'C'
        else 'D'
    end as quality_grade,
    case 
        when coalesce(100 - sus.carbon_footprint, 75) > 70 then 'Organic'
        else 'Conventional'
    end as certification,
    coalesce(sus.carbon_footprint, 50) as carbon_footprint,
    coalesce(sus.water_usage * 100, 300) as water_footprint,
    current_timestamp() as timestamp,
    current_timestamp() as created_at,
    date_diff(d.harvest_date, d.planting_date, DAY) as growing_period_days,
    (h.yield_amount * (0.5 + rand() * 0.7)) / nullif(h.yield_amount, 0) * 100 as yield_percentage,
    current_timestamp() as loaded_at
from farm_data f
join synthetic_dates d on f.farm_id = d.farm_id
join crop_data c on d.crop_id = c.crop_id
left join harvest_data h on f.farm_id = h.farm_id and c.crop_id = h.crop_id
left join yield_data y on h.harvest_id = y.harvest_id
left join sustainability_data sus on f.farm_id = sus.farm_id
left join soil_data s on f.farm_id = s.farm_id 