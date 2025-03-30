with soil_data as (
    select * from {{ ref('stg_soil') }}
),

crop_data as (
    select * from {{ ref('stg_crop') }}
),

farm_data as (
    select * from {{ ref('stg_farm') }}
),

harvest_data as (
    select * from {{ ref('stg_harvest') }}
),

yield_data as (
    select * from {{ ref('stg_yield') }}
),

soil_crop_yield as (
    select
        s.soil_id,
        s.farm_id,
        s.ph_level,
        s.nutrient_content,
        s.texture,
        s.organic_matter,
        c.crop_id,
        c.crop_name,
        c.crop_type,
        avg(y.yield_per_hectare) as avg_yield
    from soil_data s
    join farm_data f on s.farm_id = f.farm_id
    join harvest_data h on f.farm_id = h.farm_id
    join crop_data c on h.crop_id = c.crop_id
    join yield_data y on h.harvest_id = y.harvest_id
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

soil_crop_ranking as (
    select
        soil_id,
        crop_id,
        crop_name,
        avg_yield,
        rank() over (partition by soil_id order by avg_yield desc) as yield_rank
    from soil_crop_yield
),

ph_compatibility as (
    select
        crop_id,
        crop_name,
        case
            when avg(ph_level) between 5.5 and 7.0 then 'Optimal'
            when avg(ph_level) between 5.0 and 8.0 then 'Acceptable'
            else 'Suboptimal'
        end as ph_compatibility
    from soil_crop_yield
    group by 1, 2
),

texture_crop_performance as (
    select
        texture,
        crop_id,
        crop_name,
        avg(avg_yield) as texture_avg_yield,
        rank() over (partition by texture order by avg(avg_yield) desc) as texture_rank
    from soil_crop_yield
    group by 1, 2, 3
)

select
    scy.soil_id,
    scy.farm_id,
    scy.crop_id,
    scy.crop_name,
    scy.ph_level,
    scy.nutrient_content,
    scy.texture,
    scy.organic_matter,
    scy.avg_yield,
    scr.yield_rank,
    pc.ph_compatibility,
    tcp.texture_rank,
    case
        when pc.ph_compatibility = 'Optimal' and tcp.texture_rank <= 3 then 'Highly Compatible'
        when pc.ph_compatibility = 'Optimal' and tcp.texture_rank > 3 then 'Compatible'
        when pc.ph_compatibility = 'Acceptable' then 'Moderately Compatible'
        else 'Not Compatible'
    end as overall_compatibility
from soil_crop_yield scy
join soil_crop_ranking scr on scy.soil_id = scr.soil_id and scy.crop_id = scr.crop_id
join ph_compatibility pc on scy.crop_id = pc.crop_id
join texture_crop_performance tcp on scy.texture = tcp.texture and scy.crop_id = tcp.crop_id 