with crop_data as (
    select * from {{ ref('stg_crop') }}
),

production_data as (
    select * from {{ ref('stg_production') }}
),

yield_data as (
    select * from {{ ref('stg_yield') }}
),

current_year_production as (
    select
        crop_id,
        sum(quantity_produced) as total_production
    from production_data
    where extract(year from date) = extract(year from current_date())
    group by 1
),

previous_year_production as (
    select
        crop_id,
        sum(quantity_produced) as total_production
    from production_data
    where extract(year from date) = extract(year from current_date()) - 1
    group by 1
),

crop_yield as (
    select
        crop_id,
        avg(yield_per_hectare) as average_yield
    from yield_data
    group by 1
),

crop_cost as (
    select
        crop_id,
        sum(cost) as total_cost
    from production_data
    group by 1
),

crop_production as (
    select
        crop_id,
        sum(quantity_produced) as total_production
    from production_data
    group by 1
)

select
    c.crop_id,
    c.crop_name,
    cp.total_production,
    cy.average_yield,
    case
        when py.total_production is null or py.total_production = 0 then 0
        else (cy.total_production - py.total_production) / py.total_production * 100
    end as growth_rate,
    case
        when cc.total_cost = 0 then 0
        else cp.total_production / nullif(cc.total_cost, 0)
    end as profitability_index
from crop_data c
left join crop_production cp on c.crop_id = cp.crop_id
left join crop_yield cy on c.crop_id = cy.crop_id
left join current_year_production cy on c.crop_id = cy.crop_id
left join previous_year_production py on c.crop_id = py.crop_id
left join crop_cost cc on c.crop_id = cc.crop_id 