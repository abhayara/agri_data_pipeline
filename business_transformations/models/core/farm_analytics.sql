with farm_dim as (
    select * from {{ ref('stg_farm') }}
),

production_facts as (
    select * from {{ ref('stg_production') }}
),

yield_facts as (
    select * from {{ ref('stg_yield') }}
),

sustainability_facts as (
    select * from {{ ref('stg_sustainability') }}
),

farm_production as (
    select
        f.farm_id,
        f.farm_name,
        sum(p.total_revenue) as total_revenue,
        sum(p.production_cost) as total_cost,
        avg(p.profit_margin) as avg_profit_margin
    from farm_dim f
    left join production_facts p on f.farm_id = p.farm_id
    group by 1, 2
),

farm_yield as (
    select
        f.farm_id,
        f.farm_name,
        avg(y.actual_yield) as average_yield,
        sum(y.actual_yield) as total_yield
    from farm_dim f
    left join yield_facts y on f.farm_id = y.farm_id
    group by 1, 2
),

farm_sustainability as (
    select
        f.farm_id,
        f.farm_name,
        avg(s.sustainability_score) as sustainability_score,
        avg(s.carbon_footprint) as avg_carbon_footprint,
        avg(s.water_footprint) as avg_water_footprint
    from farm_dim f
    left join sustainability_facts s on f.farm_id = s.farm_id
    group by 1, 2
)

select
    f.farm_id,
    f.farm_name,
    f.farm_type,
    f.farm_size_acres,
    f.farm_location,
    f.farmer_id,
    f.farmer_name,
    fp.total_revenue,
    fp.total_cost,
    fp.avg_profit_margin,
    fy.average_yield,
    fy.total_yield,
    fs.sustainability_score,
    fs.avg_carbon_footprint,
    fs.avg_water_footprint,
    (fp.total_revenue - fp.total_cost) / nullif(fy.total_yield, 0) as production_efficiency,
    current_timestamp() as updated_at
from farm_dim f
left join farm_production fp on f.farm_id = fp.farm_id
left join farm_yield fy on f.farm_id = fy.farm_id
left join farm_sustainability fs on f.farm_id = fs.farm_id 