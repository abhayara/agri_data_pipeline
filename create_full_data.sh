#!/bin/bash

# Create the full_data table in smaller parts to avoid query complexity issues

# First, create synthetic_dates as a temporary table
bq query --use_legacy_sql=false --location=asia-south1 "
CREATE OR REPLACE TABLE \`agri-data-454414.agri_data.temp_synthetic_dates\` AS
SELECT 
    sy.farm_id,
    sy.crop_id,
    sy.year,
    CASE 
        WHEN sc.growing_season = 'Spring' THEN DATE(CAST(sy.year AS STRING) || '-03-15')
        WHEN sc.growing_season = 'Summer' THEN DATE(CAST(sy.year AS STRING) || '-06-15')
        WHEN sc.growing_season = 'Winter' THEN DATE(CAST(sy.year AS STRING) || '-11-15')
        WHEN sc.growing_season = 'Annual' THEN DATE(CAST(sy.year AS STRING) || '-04-01')
        ELSE DATE(CAST(sy.year AS STRING) || '-05-01')
    END AS planting_date,
    CASE 
        WHEN sc.growing_season = 'Spring' THEN DATE(CAST(sy.year AS STRING) || '-06-15')
        WHEN sc.growing_season = 'Summer' THEN DATE(CAST(sy.year AS STRING) || '-09-15')
        WHEN sc.growing_season = 'Winter' THEN DATE(CAST(sy.year AS STRING) || '-02-15')
        WHEN sc.growing_season = 'Annual' THEN DATE(CAST(sy.year AS STRING) || '-10-01')
        ELSE DATE(CAST(sy.year AS STRING) || '-09-01')
    END AS harvest_date
FROM \`agri-data-454414.agri_data_seeds.seed_yield\` sy
JOIN \`agri-data-454414.agri_data_seeds.seed_crop\` sc ON sy.crop_id = sc.crop_id
"

# Now create the full_data table using the temporary table
bq query --use_legacy_sql=false --location=asia-south1 "
CREATE OR REPLACE TABLE \`agri-data-454414.agri_data.full_data\` AS
SELECT
    sf.farm_id AS Farm_ID,
    sc.crop_type AS Farm_Type,
    sf.farm_size AS Farm_Size_Acres,
    CAST(sc.crop_id AS STRING) AS Crop_ID,
    d.planting_date AS Planting_Date,
    d.harvest_date AS Harvest_Date,
    sh.yield_amount AS Expected_Yield,
    sh.yield_amount * 0.9 AS Actual_Yield,
    COALESCE(ss.ph_level * 10, 60) AS Soil_Moisture,
    COALESCE(ss.ph_level * 5 + 40, 55) AS Soil_Temperature,
    COALESCE(100 - sus.carbon_footprint, 75) AS Sustainability_Score,
    EXTRACT(YEAR FROM d.planting_date) AS year,
    EXTRACT(MONTH FROM d.planting_date) AS month,
    sf.farm_location AS Farm_Location,
    sf.farm_name AS Farmer_Contact,
    'kg/hectare' AS Yield_Unit,
    COALESCE(ss.ph_level, 6.5) AS Soil_pH,
    CASE 
        WHEN ss.organic_matter > 3 THEN 'High'
        WHEN ss.organic_matter > 1.5 THEN 'Medium'
        ELSE 'Low'
    END AS Soil_Fertility,
    8 AS Weather_Sunlight,
    COALESCE(sus.water_usage * 10, 200) AS Irrigation_Amount,
    100 AS Fertilizer_Amount,
    CASE 
        WHEN sus.pesticide_usage > 50 THEN 'Chemical'
        ELSE 'Organic'
    END AS Pesticide_Type,
    COALESCE(sus.pesticide_usage, 30) AS Pesticide_Amount,
    CASE 
        WHEN sf.farm_size > 200 THEN 'Heavy Machinery'
        WHEN sf.farm_size > 100 THEN 'Tractor'
        ELSE 'Manual Tools'
    END AS Equipment_Used,
    sf.farm_size * 2 AS Labor_Hours,
    sh.yield_amount * 10 AS Total_Revenue,
    CASE 
        WHEN COALESCE(100 - sus.carbon_footprint, 75) > 80 THEN 'A'
        WHEN COALESCE(100 - sus.carbon_footprint, 75) > 60 THEN 'B'
        WHEN COALESCE(100 - sus.carbon_footprint, 75) > 40 THEN 'C'
        ELSE 'D'
    END AS Quality_Grade,
    CASE 
        WHEN COALESCE(100 - sus.carbon_footprint, 75) > 70 THEN 'Organic'
        ELSE 'Conventional'
    END AS Certification,
    COALESCE(sus.carbon_footprint, 50) AS Carbon_Footprint,
    COALESCE(sus.water_usage * 100, 300) AS Water_Footprint,
    CURRENT_TIMESTAMP() AS timestamp,
    CURRENT_TIMESTAMP() AS created_at
FROM 
    \`agri-data-454414.agri_data_seeds.seed_farm\` sf
JOIN 
    \`agri-data-454414.agri_data.temp_synthetic_dates\` d ON sf.farm_id = d.farm_id
JOIN 
    \`agri-data-454414.agri_data_seeds.seed_crop\` sc ON d.crop_id = sc.crop_id
LEFT JOIN 
    \`agri-data-454414.agri_data_seeds.seed_harvest\` sh ON sf.farm_id = sh.farm_id AND sc.crop_id = sh.crop_id
LEFT JOIN 
    \`agri-data-454414.agri_data_seeds.seed_yield\` sy ON sh.harvest_id = sy.harvest_id
LEFT JOIN 
    \`agri-data-454414.agri_data_seeds.seed_sustainability\` sus ON sf.farm_id = sus.farm_id
LEFT JOIN 
    \`agri-data-454414.agri_data_seeds.seed_soil\` ss ON sf.farm_id = ss.farm_id
"

# Clean up the temporary table
bq rm -f -t agri-data-454414:agri_data.temp_synthetic_dates

echo "✅ full_data table created successfully" 