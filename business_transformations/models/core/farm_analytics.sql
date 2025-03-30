{{ config(
    materialized = 'table'
)}}

WITH farm_production AS (
    SELECT
        f.farm_id,
        f.farm_name,
        SUM(p.quantity_produced) AS total_production,
        SUM(p.cost) AS total_cost
    FROM {{ ref('stg_farm') }} f
    LEFT JOIN {{ ref('stg_production') }} p ON f.farm_id = p.farm_id
    GROUP BY f.farm_id, f.farm_name
),

farm_yield AS (
    SELECT
        f.farm_id,
        AVG(y.yield_per_hectare) AS average_yield
    FROM {{ ref('stg_farm') }} f
    LEFT JOIN {{ ref('stg_yield') }} y ON f.farm_id = y.farm_id
    GROUP BY f.farm_id
),

farm_sustainability AS (
    -- This would be a more complex calculation in a real scenario
    -- Simplified for demonstration purposes
    SELECT
        farm_id,
        -- Create a simple sustainability score based on water, carbon, and pesticide usage
        -- In a real scenario, this would involve more complex calculations
        CASE
            WHEN farm_id % 3 = 0 THEN 'High'
            WHEN farm_id % 3 = 1 THEN 'Medium'
            ELSE 'Low'
        END AS sustainability_level
    FROM {{ ref('stg_farm') }}
)

SELECT
    fp.farm_id,
    fp.farm_name,
    fp.total_production,
    fy.average_yield,
    CASE
        WHEN fs.sustainability_level = 'High' THEN 3
        WHEN fs.sustainability_level = 'Medium' THEN 2
        ELSE 1
    END AS sustainability_score,
    SAFE_DIVIDE(fp.total_production, fp.total_cost) AS production_efficiency,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM farm_production fp
JOIN farm_yield fy ON fp.farm_id = fy.farm_id
JOIN farm_sustainability fs ON fp.farm_id = fs.farm_id 