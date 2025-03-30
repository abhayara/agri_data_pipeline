with farm_data as (
    select * from {{ ref('stg_farm') }}
),

sustainability_data as (
    select * from {{ ref('stg_sustainability') }}
),

production_data as (
    select * from {{ ref('stg_production') }}
),

water_efficiency as (
    select
        s.farm_id,
        avg(s.water_usage / nullif(p.quantity_produced, 0)) as water_usage_per_unit,
        (10 - least(avg(s.water_usage / nullif(p.quantity_produced, 0)) * 2, 10)) as water_efficiency_score
    from sustainability_data s
    left join production_data p on s.farm_id = p.farm_id and s.date = p.date
    group by 1
),

carbon_efficiency as (
    select
        s.farm_id,
        avg(s.carbon_footprint / nullif(p.quantity_produced, 0)) as carbon_footprint_per_unit,
        (10 - least(avg(s.carbon_footprint / nullif(p.quantity_produced, 0)) * 2, 10)) as carbon_efficiency_score
    from sustainability_data s
    left join production_data p on s.farm_id = p.farm_id and s.date = p.date
    group by 1
),

pesticide_efficiency as (
    select
        s.farm_id,
        avg(s.pesticide_usage / nullif(p.quantity_produced, 0)) as pesticide_usage_per_unit,
        (10 - least(avg(s.pesticide_usage / nullif(p.quantity_produced, 0)) * 3, 10)) as pesticide_efficiency_score
    from sustainability_data s
    left join production_data p on s.farm_id = p.farm_id and s.date = p.date
    group by 1
)

select
    f.farm_id,
    f.farm_name,
    we.water_efficiency_score,
    ce.carbon_efficiency_score,
    pe.pesticide_efficiency_score,
    (we.water_efficiency_score + ce.carbon_efficiency_score + pe.pesticide_efficiency_score) / 3 as overall_sustainability_score
from farm_data f
left join water_efficiency we on f.farm_id = we.farm_id
left join carbon_efficiency ce on f.farm_id = ce.farm_id
left join pesticide_efficiency pe on f.farm_id = pe.farm_id 