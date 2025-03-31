#!/bin/bash
# Script to ensure all required OLAP tables exist in BigQuery
# This will run before DBT to avoid any waiting

set -e

# Set default values for environment variables
PROJECT_ID=${GCP_PROJECT_ID:-"agri-data-454414"}
DATASET_ID=${GCP_DATASET_ID:-"agri_data"}
SEED_DATASET_ID=${SEED_DATASET_ID:-"agri_data_seeds"}
LOCATION="asia-south1"

echo "==========================================================="
echo "Ensuring OLAP tables exist in BigQuery dataset ${DATASET_ID}"
echo "==========================================================="

# Check if dataset exists, create if it doesn't
if ! bq ls --project_id=${PROJECT_ID} ${DATASET_ID} &>/dev/null; then
    echo "Creating dataset ${DATASET_ID}..."
    bq mk --location=${LOCATION} --dataset ${PROJECT_ID}:${DATASET_ID}
    echo "✅ Dataset ${DATASET_ID} created."
else
    echo "✅ Dataset ${DATASET_ID} already exists."
fi

# Function to check if a table exists
check_table_exists() {
    local table_name=$1
    bq ls --project_id=${PROJECT_ID} ${DATASET_ID} 2>/dev/null | grep -q "${table_name}"
    return $?
}

# Create farm_analysis table if it doesn't exist
if ! check_table_exists "farm_analysis"; then
    echo "Creating farm_analysis table..."
    bq query --use_legacy_sql=false --location=${LOCATION} "
    CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET_ID}.farm_analysis\` AS
    SELECT
        sf.farm_id AS Farm_ID,
        'Mixed' AS Farm_Type,
        sf.farm_size AS avg_Farm_Size_Acres,
        COALESCE(ss.ph_level * 10, 60) AS avg_Soil_Moisture,
        COALESCE(ss.ph_level * 5 + 40, 55) AS avg_Soil_Temperature
    FROM 
        \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_farm\` sf
    LEFT JOIN 
        \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_soil\` ss ON sf.farm_id = ss.farm_id
    "
    echo "✅ farm_analysis table created."
else
    echo "✅ farm_analysis table already exists."
fi

# Create crop_analysis table if it doesn't exist
if ! check_table_exists "crop_analysis"; then
    echo "Creating crop_analysis table..."
    bq query --use_legacy_sql=false --location=${LOCATION} "
    CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET_ID}.crop_analysis\` AS
    SELECT
        sc.crop_type AS Crop_Type,
        sc.crop_name AS Crop_Variety,
        100 AS avg_Expected_Yield,
        90 AS avg_Actual_Yield,
        CASE
            WHEN sc.growing_season = 'Annual' THEN 365
            WHEN sc.growing_season = 'Spring' THEN 90
            WHEN sc.growing_season = 'Summer' THEN 100
            WHEN sc.growing_season = 'Fall' THEN 80
            WHEN sc.growing_season = 'Winter' THEN 120
            ELSE 90
        END AS avg_Growing_Period_Days
    FROM 
        \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_crop\` sc
    "
    echo "✅ crop_analysis table created."
else
    echo "✅ crop_analysis table already exists."
fi

# Create synthetic_dates temp table for full_data if needed
echo "Creating temporary synthetic_dates table..."
bq query --use_legacy_sql=false --location=${LOCATION} "
CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET_ID}.temp_synthetic_dates\` AS
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
FROM \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_yield\` sy
JOIN \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_crop\` sc ON sy.crop_id = sc.crop_id
"

# Create full_data table if it doesn't exist
if ! check_table_exists "full_data"; then
    echo "Creating full_data table..."
    bq query --use_legacy_sql=false --location=${LOCATION} "
    CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET_ID}.full_data\` AS
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
        \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_farm\` sf
    JOIN 
        \`${PROJECT_ID}.${DATASET_ID}.temp_synthetic_dates\` d ON sf.farm_id = d.farm_id
    JOIN 
        \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_crop\` sc ON d.crop_id = sc.crop_id
    LEFT JOIN 
        \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_harvest\` sh ON sf.farm_id = sh.farm_id AND sc.crop_id = sh.crop_id
    LEFT JOIN 
        \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_yield\` sy ON sh.harvest_id = sy.harvest_id
    LEFT JOIN 
        \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_sustainability\` sus ON sf.farm_id = sus.farm_id
    LEFT JOIN 
        \`${PROJECT_ID}.${SEED_DATASET_ID}.seed_soil\` ss ON sf.farm_id = ss.farm_id
    LIMIT 100
    "
    echo "✅ full_data table created."
else
    echo "✅ full_data table already exists."
fi

# Clean up temporary table
echo "Cleaning up temporary table..."
bq rm -f -t ${PROJECT_ID}:${DATASET_ID}.temp_synthetic_dates

echo "==========================================================="
echo "✅ All required OLAP tables are ready for DBT processing!"
echo "===========================================================" 