with crop_dim as (
    select * from {{ ref('stg_crop') }}
),

yield_facts as (
    select * from {{ ref('stg_yield') }}
),

production_facts as (
    select * from {{ ref('stg_production') }}
),

crop_yield as (
    select
        c.crop_id,
        c.crop_type,
        c.crop_variety,
        avg(y.actual_yield) as average_yield,
        sum(y.actual_yield) as total_production,
        avg(y.yield_ratio) as yield_efficiency
    from crop_dim c
    left join yield_facts y on c.crop_id = y.crop_id
    group by 1, 2, 3
),

crop_economics as (
    select
        c.crop_id,
        avg(p.market_price) as avg_market_price,
        sum(p.total_revenue) as total_revenue,
        avg(p.profit_margin) as avg_profit_margin
    from crop_dim c
    left join production_facts p on c.crop_id = p.crop_id
    group by 1
),

-- Calculate year-over-year growth using window functions
crop_growth as (
    select
        crop_id,
        extract(year from harvest_date) as harvest_year,
        sum(actual_yield) as yearly_production
    from yield_facts
    where harvest_date is not null
    group by 1, 2
),

crop_growth_rate as (
    select
        crop_id,
        harvest_year,
        yearly_production,
        lag(yearly_production) over (partition by crop_id order by harvest_year) as prev_year_production,
        case
            when lag(yearly_production) over (partition by crop_id order by harvest_year) is not null
            and lag(yearly_production) over (partition by crop_id order by harvest_year) != 0
            then (yearly_production - lag(yearly_production) over (partition by crop_id order by harvest_year)) / 
                 lag(yearly_production) over (partition by crop_id order by harvest_year) * 100
            else 0
        end as growth_rate
    from crop_growth
),

crop_avg_growth as (
    select
        crop_id,
        avg(growth_rate) as avg_growth_rate
    from crop_growth_rate
    group by 1
)

select
    c.crop_id,
    c.crop_type,
    c.crop_variety as crop_name,
    cy.average_yield,
    cy.total_production,
    cy.yield_efficiency,
    ce.avg_market_price,
    ce.total_revenue,
    ce.avg_profit_margin,
    coalesce(cg.avg_growth_rate, 0) as growth_rate,
    (ce.avg_profit_margin * cy.yield_efficiency) as profitability_index,
    current_timestamp() as updated_at
from crop_dim c
left join crop_yield cy on c.crop_id = cy.crop_id
left join crop_economics ce on c.crop_id = ce.crop_id
left join crop_avg_growth cg on c.crop_id = cg.crop_id 