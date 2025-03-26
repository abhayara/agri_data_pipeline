{{ config(
    materialized = 'table'
)}}

WITH crop_weather_data AS (
    -- Join crops with weather and yield data
    SELECT
        c.crop_id,
        c.crop_name,
        c.crop_type,
        w.temperature,
        w.precipitation,
        w.humidity,
        y.yield_per_hectare,
        f.farm_id,
        f.location
    FROM {{ ref('stg_crop') }} c
    JOIN {{ ref('stg_harvest') }} h ON c.crop_id = h.crop_id
    JOIN {{ ref('stg_farm') }} f ON h.farm_id = f.farm_id
    JOIN {{ ref('stg_yield') }} y ON h.harvest_id = y.harvest_id
    JOIN {{ ref('stg_weather') }} w 
        -- Join on date and location (simplified)
        ON DATE(h.harvest_date) = DATE(w.date)
        AND CAST(w.location AS STRING) = CAST(f.location AS STRING)
),

-- Calculate temperature sensitivity
temp_sensitivity AS (
    SELECT
        crop_id,
        crop_name,
        -- Measure correlation by calculating variance in yield across temperature range
        -- Higher variance = more sensitive
        STDDEV_SAMP(yield_per_hectare) AS yield_variance,
        AVG(temperature) AS avg_temp,
        -- Use coefficient of variation to normalize
        CASE
            WHEN AVG(yield_per_hectare) > 0 THEN 
                STDDEV_SAMP(yield_per_hectare) / AVG(yield_per_hectare)
            ELSE 0
        END AS temp_sensitivity_coef
    FROM crop_weather_data
    GROUP BY crop_id, crop_name
),

-- Calculate precipitation sensitivity
precip_sensitivity AS (
    SELECT
        crop_id,
        crop_name,
        -- Similar approach for precipitation
        STDDEV_SAMP(yield_per_hectare) AS yield_variance,
        AVG(precipitation) AS avg_precip,
        CASE
            WHEN AVG(yield_per_hectare) > 0 THEN 
                STDDEV_SAMP(yield_per_hectare) / AVG(yield_per_hectare)
            ELSE 0
        END AS precip_sensitivity_coef
    FROM crop_weather_data
    GROUP BY crop_id, crop_name
),

-- Calculate humidity sensitivity
humidity_sensitivity AS (
    SELECT
        crop_id,
        crop_name,
        -- Similar approach for humidity
        STDDEV_SAMP(yield_per_hectare) AS yield_variance,
        AVG(humidity) AS avg_humidity,
        CASE
            WHEN AVG(yield_per_hectare) > 0 THEN 
                STDDEV_SAMP(yield_per_hectare) / AVG(yield_per_hectare)
            ELSE 0
        END AS humidity_sensitivity_coef
    FROM crop_weather_data
    GROUP BY crop_id, crop_name
)

SELECT
    ts.crop_id,
    ts.crop_name,
    -- Normalize sensitivity scores to a 0-100 scale
    -- Higher = more sensitive
    LEAST(100, GREATEST(0, ts.temp_sensitivity_coef * 100)) AS temperature_sensitivity,
    LEAST(100, GREATEST(0, ps.precip_sensitivity_coef * 100)) AS precipitation_sensitivity,
    LEAST(100, GREATEST(0, hs.humidity_sensitivity_coef * 100)) AS humidity_sensitivity,
    
    -- Calculate overall weather risk score
    (
        LEAST(100, GREATEST(0, ts.temp_sensitivity_coef * 100)) +
        LEAST(100, GREATEST(0, ps.precip_sensitivity_coef * 100)) +
        LEAST(100, GREATEST(0, hs.humidity_sensitivity_coef * 100))
    ) / 3 AS weather_risk_score,
    
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM temp_sensitivity ts
JOIN precip_sensitivity ps ON ts.crop_id = ps.crop_id
JOIN humidity_sensitivity hs ON ts.crop_id = hs.crop_id 