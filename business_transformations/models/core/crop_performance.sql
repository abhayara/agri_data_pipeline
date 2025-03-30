{{ config(
    materialized = 'table'
)}}

WITH crop_production AS (
    SELECT
        c.crop_id,
        c.crop_name,
        SUM(p.quantity_produced) AS total_production,
        SUM(p.cost) AS total_cost
    FROM {{ ref('stg_crop') }} c
    LEFT JOIN {{ ref('stg_production') }} p ON c.crop_id = p.crop_id
    GROUP BY c.crop_id, c.crop_name
),

crop_yield AS (
    SELECT
        c.crop_id,
        AVG(y.yield_per_hectare) AS average_yield
    FROM {{ ref('stg_crop') }} c
    LEFT JOIN {{ ref('stg_yield') }} y ON c.crop_id = y.crop_id
    GROUP BY c.crop_id
),

crop_growth AS (
    -- This would be a more complex calculation with real data
    -- Simplified for demonstration purposes
    SELECT
        c.crop_id,
        -- Simulating growth rate calculation
        RAND() * 20 - 5 AS growth_rate  -- Random value between -5% and 15%
    FROM {{ ref('stg_crop') }} c
)

SELECT
    cp.crop_id,
    cp.crop_name,
    cp.total_production,
    cy.average_yield,
    cg.growth_rate,
    -- Calculating a simple profitability index
    -- In a real scenario, this would be a more complex calculation
    SAFE_DIVIDE(cp.total_production, cp.total_cost) * 100 AS profitability_index,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM crop_production cp
JOIN crop_yield cy ON cp.crop_id = cy.crop_id
JOIN crop_growth cg ON cp.crop_id = cg.crop_id 