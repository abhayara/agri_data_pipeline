with farm_data as (
    select * from {{ ref('stg_farm') }}
),

production_data as (
    select * from {{ ref('stg_production') }}
),

yield_data as (
    select * from {{ ref('stg_yield') }}
),

sustainability_data as (
    select * from {{ ref('stg_sustainability') }}
),

farm_production as (
    select
        f.farm_id,
        f.farm_name,
        sum(p.quantity_produced) as total_production,
        sum(p.cost) as total_cost
    from farm_data f
    left join production_data p on f.farm_id = p.farm_id
    group by 1, 2
),

farm_yield as (
    select
        f.farm_id,
        avg(y.yield_per_hectare) as average_yield
    from farm_data f
    left join yield_data y on f.farm_id = y.farm_id
    group by 1
),

farm_sustainability as (
    select
        f.farm_id,
        avg(s.water_usage / nullif(p.quantity_produced, 0)) as water_efficiency,
        avg(s.carbon_footprint / nullif(p.quantity_produced, 0)) as carbon_efficiency,
        avg(s.pesticide_usage / nullif(p.quantity_produced, 0)) as pesticide_efficiency,
        (
            10 - (
                coalesce(avg(s.water_usage / nullif(p.quantity_produced, 0)), 0) * 2 +
                coalesce(avg(s.carbon_footprint / nullif(p.quantity_produced, 0)), 0) * 2 +
                coalesce(avg(s.pesticide_usage / nullif(p.quantity_produced, 0)), 0) * 2
            ) / 3
        ) as sustainability_score
    from farm_data f
    left join sustainability_data s on f.farm_id = s.farm_id
    left join production_data p on f.farm_id = p.farm_id and p.date = s.date
    group by 1
)

select
    fp.farm_id,
    fp.farm_name,
    fp.total_production,
    fy.average_yield,
    fs.sustainability_score,
    case 
        when fp.total_cost = 0 then 0
        else fp.total_production / nullif(fp.total_cost, 0)
    end as production_efficiency
from farm_production fp
left join farm_yield fy on fp.farm_id = fy.farm_id
left join farm_sustainability fs on fp.farm_id = fs.farm_id 