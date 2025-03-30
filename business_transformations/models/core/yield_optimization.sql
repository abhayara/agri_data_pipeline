{{ config(
    materialized = 'table'
)}}

WITH crop_yields AS (
    SELECT
        f.farm_id,
        f.farm_name,
        c.crop_id,
        c.crop_name,
        y.yield_per_hectare AS current_yield,
        -- Additional data about the yield
        h.harvest_date,
        h.quality_grade
    FROM {{ ref('stg_farm') }} f
    JOIN {{ ref('stg_harvest') }} h ON f.farm_id = h.farm_id
    JOIN {{ ref('stg_crop') }} c ON h.crop_id = c.crop_id
    JOIN {{ ref('stg_yield') }} y ON h.harvest_id = y.harvest_id
),

-- Calculate average and top yields by crop
crop_benchmarks AS (
    SELECT
        crop_id,
        crop_name,
        AVG(current_yield) AS avg_yield,
        MAX(current_yield) AS max_yield
    FROM crop_yields
    GROUP BY crop_id, crop_name
),

-- Weather impacts on yield
weather_impacts AS (
    SELECT
        cy.farm_id,
        cy.crop_id,
        cy.harvest_date,
        w.temperature,
        w.precipitation,
        w.humidity,
        cy.current_yield
    FROM crop_yields cy
    LEFT JOIN {{ ref('stg_weather') }} w 
        ON DATE(cy.harvest_date) = DATE(w.date)
        -- Simplified location match - in reality would be more complex geospatial matching
        AND CAST(w.location AS STRING) = CAST(cy.farm_id AS STRING)
),

-- Soil impacts on yield
soil_impacts AS (
    SELECT
        cy.farm_id,
        cy.crop_id,
        s.ph_level,
        s.nutrient_content,
        s.texture,
        s.organic_matter,
        cy.current_yield
    FROM crop_yields cy
    LEFT JOIN {{ ref('stg_soil') }} s ON cy.farm_id = s.farm_id
)

SELECT
    cy.farm_id,
    cy.crop_id,
    cy.farm_name,
    cy.crop_name,
    cy.current_yield,
    -- Calculate optimal yield based on benchmarks and local conditions
    CASE
        -- If current yield is already at max, keep it
        WHEN cy.current_yield >= cb.max_yield THEN cy.current_yield
        -- Otherwise suggest improvement to at least average yield + 10%
        ELSE GREATEST(cb.avg_yield * 1.1, cy.current_yield * 1.2)
    END AS optimal_yield,
    
    -- Calculate yield gap
    CASE
        WHEN cy.current_yield >= cb.max_yield THEN 0
        ELSE GREATEST(cb.avg_yield * 1.1, cy.current_yield * 1.2) - cy.current_yield
    END AS yield_gap,
    
    -- Generate recommendations
    CASE
        WHEN cy.current_yield >= cb.max_yield THEN 'Current yield is already optimal'
        WHEN wi.temperature IS NULL OR si.ph_level IS NULL THEN 'Insufficient data for detailed recommendations'
        -- Sample recommendations based on weather and soil data
        WHEN wi.precipitation < 50 AND cy.current_yield < cb.avg_yield THEN 'Low rainfall detected. Consider improved irrigation'
        WHEN si.nutrient_content < 30 AND cy.current_yield < cb.avg_yield THEN 'Low soil nutrients. Consider fertilization'
        WHEN si.ph_level < 5.5 AND cy.current_yield < cb.avg_yield THEN 'Soil pH is too acidic. Consider lime application'
        WHEN si.ph_level > 7.5 AND cy.current_yield < cb.avg_yield THEN 'Soil pH is too alkaline. Consider sulfur application'
        ELSE 'Review crop management practices to improve yield'
    END AS optimization_recommendations,
    
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM crop_yields cy
JOIN crop_benchmarks cb ON cy.crop_id = cb.crop_id
LEFT JOIN weather_impacts wi ON cy.farm_id = wi.farm_id AND cy.crop_id = wi.crop_id
LEFT JOIN soil_impacts si ON cy.farm_id = si.farm_id AND cy.crop_id = si.crop_id 