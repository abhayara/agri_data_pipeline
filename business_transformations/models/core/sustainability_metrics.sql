{{ config(
    materialized = 'table'
)}}

WITH farm_sustainability AS (
    SELECT
        f.farm_id,
        f.farm_name,
        AVG(s.water_usage) AS avg_water_usage,
        AVG(s.carbon_footprint) AS avg_carbon_footprint,
        AVG(s.pesticide_usage) AS avg_pesticide_usage
    FROM {{ ref('stg_farm') }} f
    LEFT JOIN {{ ref('stg_sustainability') }} s ON f.farm_id = s.farm_id
    GROUP BY f.farm_id, f.farm_name
),

-- Calculate industry averages
industry_averages AS (
    SELECT
        AVG(avg_water_usage) AS industry_avg_water,
        AVG(avg_carbon_footprint) AS industry_avg_carbon,
        AVG(avg_pesticide_usage) AS industry_avg_pesticide
    FROM farm_sustainability
)

SELECT
    fs.farm_id,
    fs.farm_name,
    fs.avg_water_usage,
    fs.avg_carbon_footprint,
    fs.avg_pesticide_usage,
    -- Calculate efficiency scores (lower is better, normalized to 0-100 scale)
    -- Water efficiency: how much better than industry average (100 = best)
    CASE
        WHEN fs.avg_water_usage > 0 THEN 
            LEAST(100, GREATEST(0, 100 - (100 * (fs.avg_water_usage - ia.industry_avg_water) / NULLIF(ia.industry_avg_water, 0))))
        ELSE 100
    END AS water_efficiency,
    
    -- Carbon efficiency
    CASE
        WHEN fs.avg_carbon_footprint > 0 THEN 
            LEAST(100, GREATEST(0, 100 - (100 * (fs.avg_carbon_footprint - ia.industry_avg_carbon) / NULLIF(ia.industry_avg_carbon, 0))))
        ELSE 100
    END AS carbon_efficiency,
    
    -- Pesticide efficiency
    CASE
        WHEN fs.avg_pesticide_usage > 0 THEN 
            LEAST(100, GREATEST(0, 100 - (100 * (fs.avg_pesticide_usage - ia.industry_avg_pesticide) / NULLIF(ia.industry_avg_pesticide, 0))))
        ELSE 100
    END AS pesticide_efficiency,
    
    -- Overall sustainability score (average of all efficiency scores)
    (
        CASE
            WHEN fs.avg_water_usage > 0 THEN 
                LEAST(100, GREATEST(0, 100 - (100 * (fs.avg_water_usage - ia.industry_avg_water) / NULLIF(ia.industry_avg_water, 0))))
            ELSE 100
        END +
        CASE
            WHEN fs.avg_carbon_footprint > 0 THEN 
                LEAST(100, GREATEST(0, 100 - (100 * (fs.avg_carbon_footprint - ia.industry_avg_carbon) / NULLIF(ia.industry_avg_carbon, 0))))
            ELSE 100
        END +
        CASE
            WHEN fs.avg_pesticide_usage > 0 THEN 
                LEAST(100, GREATEST(0, 100 - (100 * (fs.avg_pesticide_usage - ia.industry_avg_pesticide) / NULLIF(ia.industry_avg_pesticide, 0))))
            ELSE 100
        END
    ) / 3 AS overall_sustainability_score,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM farm_sustainability fs
CROSS JOIN industry_averages ia 