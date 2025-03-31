with farm_dim as (
    select * from {{ ref('stg_farm') }}
),

sustainability_facts as (
    select * from {{ ref('stg_sustainability') }}
),

production_facts as (
    select * from {{ ref('stg_production') }}
),

-- Calculate water efficiency
water_efficiency as (
    select
        s.farm_id,
        -- Lower water footprint is better for efficiency
        case 
            when avg(s.water_footprint) > 0 
            then 10 - (avg(s.water_footprint) / 500) -- Normalize to 0-10 scale (lower is better)
            else 10
        end as water_efficiency_score
    from sustainability_facts s
    group by 1
),

-- Calculate carbon efficiency
carbon_efficiency as (
    select
        s.farm_id,
        -- Lower carbon footprint is better for efficiency
        case 
            when avg(s.carbon_footprint) > 0 
            then 10 - (avg(s.carbon_footprint) / 100) -- Normalize to 0-10 scale (lower is better)
            else 10 
        end as carbon_efficiency_score
    from sustainability_facts s
    group by 1
),

-- Calculate pesticide efficiency
pesticide_efficiency as (
    select
        p.farm_id,
        -- Lower pesticide amount or organic types are better
        case
            when p.pesticide_type = 'None' then 10
            when p.pesticide_type = 'Organic' then 8
            when p.pesticide_type = 'Mixed' then 5
            when p.pesticide_type = 'Chemical' and p.pesticide_amount < 10 then 3
            when p.pesticide_type = 'Chemical' and p.pesticide_amount >= 10 then 1
            else 5
        end as pesticide_score
    from production_facts p
),

avg_pesticide_efficiency as (
    select
        farm_id,
        avg(pesticide_score) as pesticide_efficiency_score
    from pesticide_efficiency
    group by 1
)

select
    f.farm_id,
    f.farm_name,
    f.farm_type,
    f.farm_size_acres,
    w.water_efficiency_score,
    c.carbon_efficiency_score,
    p.pesticide_efficiency_score,
    (w.water_efficiency_score * 0.4 + 
     c.carbon_efficiency_score * 0.4 + 
     p.pesticide_efficiency_score * 0.2) as overall_sustainability_score,
    case
        when (w.water_efficiency_score * 0.4 + 
             c.carbon_efficiency_score * 0.4 + 
             p.pesticide_efficiency_score * 0.2) >= 8 then 'Excellent'
        when (w.water_efficiency_score * 0.4 + 
             c.carbon_efficiency_score * 0.4 + 
             p.pesticide_efficiency_score * 0.2) >= 6 then 'Good'
        when (w.water_efficiency_score * 0.4 + 
             c.carbon_efficiency_score * 0.4 + 
             p.pesticide_efficiency_score * 0.2) >= 4 then 'Average'
        else 'Needs Improvement'
    end as sustainability_rating,
    current_timestamp() as updated_at
from farm_dim f
left join water_efficiency w on f.farm_id = w.farm_id
left join carbon_efficiency c on f.farm_id = c.farm_id
left join avg_pesticide_efficiency p on f.farm_id = p.farm_id 