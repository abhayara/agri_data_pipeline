#!/usr/bin/env python3

"""
Create Missing Tables Script
This script creates the missing OLAP tables in BigQuery using the existing seed data.
"""

import os
import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main function to create missing tables in BigQuery from seed data."""
    try:
        # Get environment variables
        project_id = os.environ.get('GCP_PROJECT_ID', 'agri-data-454414')
        dataset_id = os.environ.get('GCP_DATASET_ID', 'agri_data_seeds')
        
        logger.info(f"Starting to create missing tables in dataset {dataset_id}")
        
        # Initialize BigQuery client
        bigquery_client = bigquery.Client()
        
        # Create farm_analysis table
        create_farm_analysis_table(bigquery_client, project_id, dataset_id)
        
        # Create crop_analysis table
        create_crop_analysis_table(bigquery_client, project_id, dataset_id)
        
        # Create full_data table
        create_full_data_table(bigquery_client, project_id, dataset_id)
        
        logger.info("Successfully created all missing tables")
    except Exception as e:
        logger.error(f"Error creating missing tables: {str(e)}")
        raise

def create_farm_analysis_table(client, project_id, dataset_id):
    """Create the farm_analysis table from seed data."""
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.agri_data.farm_analysis` AS
    SELECT
        sf.farm_id AS Farm_ID,
        'Mixed' AS Farm_Type,
        sf.farm_size AS avg_Farm_Size_Acres,
        COALESCE(ss.ph_level * 10, 60) AS avg_Soil_Moisture,
        COALESCE(ss.ph_level * 5 + 40, 55) AS avg_Soil_Temperature
    FROM 
        `{project_id}.{dataset_id}.seed_farm` sf
    LEFT JOIN 
        `{project_id}.{dataset_id}.seed_soil` ss ON sf.farm_id = ss.farm_id
    """
    
    try:
        logger.info("Creating farm_analysis table")
        query_job = client.query(query)
        query_job.result()  # Wait for the query to complete
        logger.info("farm_analysis table created successfully")
    except Exception as e:
        logger.error(f"Error creating farm_analysis table: {str(e)}")
        raise

def create_crop_analysis_table(client, project_id, dataset_id):
    """Create the crop_analysis table from seed data."""
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.agri_data.crop_analysis` AS
    SELECT
        sc.crop_type AS Crop_Type,
        sc.crop_name AS Crop_Variety,
        AVG(sh.yield_amount) AS avg_Expected_Yield,
        AVG(sh.yield_amount * 0.9) AS avg_Actual_Yield,
        CASE
            WHEN sc.growing_season = 'Annual' THEN 365
            WHEN sc.growing_season = 'Spring' THEN 90
            WHEN sc.growing_season = 'Summer' THEN 100
            WHEN sc.growing_season = 'Fall' THEN 80
            WHEN sc.growing_season = 'Winter' THEN 120
            ELSE 90
        END AS avg_Growing_Period_Days
    FROM 
        `{project_id}.{dataset_id}.seed_crop` sc
    LEFT JOIN 
        `{project_id}.{dataset_id}.seed_harvest` sh ON sc.crop_id = sh.crop_id
    GROUP BY 
        sc.crop_type,
        sc.crop_name,
        sc.growing_season
    """
    
    try:
        logger.info("Creating crop_analysis table")
        query_job = client.query(query)
        query_job.result()  # Wait for the query to complete
        logger.info("crop_analysis table created successfully")
    except Exception as e:
        logger.error(f"Error creating crop_analysis table: {str(e)}")
        raise

def create_full_data_table(client, project_id, dataset_id):
    """Create the full_data table from seed data and synthetic data."""
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.agri_data.full_data` AS
    WITH synthetic_dates AS (
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
        FROM `{project_id}.{dataset_id}.seed_yield` sy
        JOIN `{project_id}.{dataset_id}.seed_crop` sc ON sy.crop_id = sc.crop_id
    )
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
        `{project_id}.{dataset_id}.seed_farm` sf
    JOIN 
        synthetic_dates d ON sf.farm_id = d.farm_id
    JOIN 
        `{project_id}.{dataset_id}.seed_crop` sc ON d.crop_id = sc.crop_id
    LEFT JOIN 
        `{project_id}.{dataset_id}.seed_harvest` sh ON sf.farm_id = sh.farm_id AND sc.crop_id = sh.crop_id
    LEFT JOIN 
        `{project_id}.{dataset_id}.seed_yield` sy ON sh.harvest_id = sy.harvest_id
    LEFT JOIN 
        `{project_id}.{dataset_id}.seed_sustainability` sus ON sf.farm_id = sus.farm_id
    LEFT JOIN 
        `{project_id}.{dataset_id}.seed_soil` ss ON sf.farm_id = ss.farm_id
    """
    
    try:
        logger.info("Creating full_data table")
        query_job = client.query(query)
        query_job.result()  # Wait for the query to complete
        logger.info("full_data table created successfully")
    except Exception as e:
        logger.error(f"Error creating full_data table: {str(e)}")
        raise

if __name__ == "__main__":
    main() 