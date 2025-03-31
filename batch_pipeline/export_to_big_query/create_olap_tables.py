#!/usr/bin/env python3

"""
Create OLAP Tables Script
This script creates the required OLAP tables in BigQuery using the seed data when the real OLAP data is not available.
"""

import os
import logging
import subprocess
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main function to create OLAP tables in BigQuery from seed data."""
    try:
        # Get environment variables
        project_id = os.environ.get('GCP_PROJECT_ID', 'agri-data-454414')
        dataset_id = os.environ.get('GCP_DATASET_ID', 'agri_data')
        seed_dataset_id = os.environ.get('SEED_DATASET_ID', 'agri_data_seeds')
        
        logger.info(f"Starting to create OLAP tables in dataset {dataset_id}")
        
        # Check if the required OLAP tables already exist
        olap_tables_exist = check_olap_tables_exist(project_id, dataset_id)
        
        if olap_tables_exist:
            logger.info("OLAP tables already exist, skipping creation")
            return
        
        # Create the OLAP tables
        create_olap_tables(project_id, dataset_id, seed_dataset_id)
        
        logger.info("Successfully created all OLAP tables")
    except Exception as e:
        logger.error(f"Error creating OLAP tables: {str(e)}")
        raise

def check_olap_tables_exist(project_id, dataset_id):
    """Check if the required OLAP tables already exist."""
    required_tables = ["farm_analysis", "crop_analysis", "full_data"]
    
    try:
        # Use bq command to list tables
        result = subprocess.run(
            ["bq", "ls", "--format=json", f"{project_id}:{dataset_id}"],
            capture_output=True,
            text=True,
            check=True
        )
        
        # If no tables exist, the output might be empty or an error
        if "tableId" not in result.stdout:
            return False
        
        # Check if all required tables exist
        for table in required_tables:
            table_check = subprocess.run(
                ["bq", "show", f"{project_id}:{dataset_id}.{table}"],
                capture_output=True,
                text=True
            )
            
            if table_check.returncode != 0:
                logger.info(f"Table {table} does not exist")
                return False
        
        return True
    except Exception as e:
        logger.warning(f"Error checking existing tables: {str(e)}")
        return False

def create_olap_tables(project_id, dataset_id, seed_dataset_id):
    """Create the OLAP tables from seed data."""
    try:
        # Create farm_analysis
        logger.info("Creating farm_analysis table")
        subprocess.run([
            "bq", "query", "--use_legacy_sql=false", "--location=asia-south1",
            f"CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.farm_analysis` AS "
            f"SELECT sf.farm_id AS Farm_ID, 'Mixed' AS Farm_Type, sf.farm_size AS avg_Farm_Size_Acres, "
            f"COALESCE(ss.ph_level * 10, 60) AS avg_Soil_Moisture, "
            f"COALESCE(ss.ph_level * 5 + 40, 55) AS avg_Soil_Temperature "
            f"FROM `{project_id}.{seed_dataset_id}.seed_farm` sf "
            f"LEFT JOIN `{project_id}.{seed_dataset_id}.seed_soil` ss ON sf.farm_id = ss.farm_id"
        ], check=True)
        logger.info("Created farm_analysis table")
        
        # Create crop_analysis
        logger.info("Creating crop_analysis table")
        subprocess.run([
            "bq", "query", "--use_legacy_sql=false", "--location=asia-south1",
            f"CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.crop_analysis` AS "
            f"SELECT sc.crop_type AS Crop_Type, sc.crop_name AS Crop_Variety, "
            f"100 AS avg_Expected_Yield, 90 AS avg_Actual_Yield, "
            f"CASE WHEN sc.growing_season = 'Annual' THEN 365 "
            f"WHEN sc.growing_season = 'Spring' THEN 90 "
            f"WHEN sc.growing_season = 'Summer' THEN 100 "
            f"WHEN sc.growing_season = 'Fall' THEN 80 "
            f"WHEN sc.growing_season = 'Winter' THEN 120 "
            f"ELSE 90 END AS avg_Growing_Period_Days "
            f"FROM `{project_id}.{seed_dataset_id}.seed_crop` sc"
        ], check=True)
        logger.info("Created crop_analysis table")
        
        # Create temp_synthetic_dates for full_data
        logger.info("Creating temporary synthetic_dates table")
        subprocess.run([
            "bq", "query", "--use_legacy_sql=false", "--location=asia-south1",
            f"CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.temp_synthetic_dates` AS "
            f"SELECT sy.farm_id, sy.crop_id, sy.year, "
            f"CASE WHEN sc.growing_season = 'Spring' THEN DATE(CAST(sy.year AS STRING) || '-03-15') "
            f"WHEN sc.growing_season = 'Summer' THEN DATE(CAST(sy.year AS STRING) || '-06-15') "
            f"WHEN sc.growing_season = 'Winter' THEN DATE(CAST(sy.year AS STRING) || '-11-15') "
            f"WHEN sc.growing_season = 'Annual' THEN DATE(CAST(sy.year AS STRING) || '-04-01') "
            f"ELSE DATE(CAST(sy.year AS STRING) || '-05-01') END AS planting_date, "
            f"CASE WHEN sc.growing_season = 'Spring' THEN DATE(CAST(sy.year AS STRING) || '-06-15') "
            f"WHEN sc.growing_season = 'Summer' THEN DATE(CAST(sy.year AS STRING) || '-09-15') "
            f"WHEN sc.growing_season = 'Winter' THEN DATE(CAST(sy.year AS STRING) || '-02-15') "
            f"WHEN sc.growing_season = 'Annual' THEN DATE(CAST(sy.year AS STRING) || '-10-01') "
            f"ELSE DATE(CAST(sy.year AS STRING) || '-09-01') END AS harvest_date "
            f"FROM `{project_id}.{seed_dataset_id}.seed_yield` sy "
            f"JOIN `{project_id}.{seed_dataset_id}.seed_crop` sc ON sy.crop_id = sc.crop_id"
        ], check=True)
        logger.info("Created temporary synthetic_dates table")
        
        # Create full_data
        logger.info("Creating full_data table")
        subprocess.run([
            "bq", "query", "--use_legacy_sql=false", "--location=asia-south1",
            f"CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.full_data` AS "
            f"SELECT sf.farm_id AS Farm_ID, sc.crop_type AS Farm_Type, sf.farm_size AS Farm_Size_Acres, "
            f"CAST(sc.crop_id AS STRING) AS Crop_ID, d.planting_date AS Planting_Date, "
            f"d.harvest_date AS Harvest_Date, sh.yield_amount AS Expected_Yield, "
            f"sh.yield_amount * 0.9 AS Actual_Yield, COALESCE(ss.ph_level * 10, 60) AS Soil_Moisture, "
            f"COALESCE(ss.ph_level * 5 + 40, 55) AS Soil_Temperature, "
            f"COALESCE(100 - sus.carbon_footprint, 75) AS Sustainability_Score, "
            f"EXTRACT(YEAR FROM d.planting_date) AS year, EXTRACT(MONTH FROM d.planting_date) AS month, "
            f"sf.farm_location AS Farm_Location, sf.farm_name AS Farmer_Contact, 'kg/hectare' AS Yield_Unit, "
            f"COALESCE(ss.ph_level, 6.5) AS Soil_pH, CASE WHEN ss.organic_matter > 3 THEN 'High' "
            f"WHEN ss.organic_matter > 1.5 THEN 'Medium' ELSE 'Low' END AS Soil_Fertility, "
            f"8 AS Weather_Sunlight, COALESCE(sus.water_usage * 10, 200) AS Irrigation_Amount, "
            f"100 AS Fertilizer_Amount, CASE WHEN sus.pesticide_usage > 50 THEN 'Chemical' "
            f"ELSE 'Organic' END AS Pesticide_Type, COALESCE(sus.pesticide_usage, 30) AS Pesticide_Amount, "
            f"CASE WHEN sf.farm_size > 200 THEN 'Heavy Machinery' "
            f"WHEN sf.farm_size > 100 THEN 'Tractor' ELSE 'Manual Tools' END AS Equipment_Used, "
            f"sf.farm_size * 2 AS Labor_Hours, sh.yield_amount * 10 AS Total_Revenue, "
            f"CASE WHEN COALESCE(100 - sus.carbon_footprint, 75) > 80 THEN 'A' "
            f"WHEN COALESCE(100 - sus.carbon_footprint, 75) > 60 THEN 'B' "
            f"WHEN COALESCE(100 - sus.carbon_footprint, 75) > 40 THEN 'C' ELSE 'D' END AS Quality_Grade, "
            f"CASE WHEN COALESCE(100 - sus.carbon_footprint, 75) > 70 THEN 'Organic' "
            f"ELSE 'Conventional' END AS Certification, COALESCE(sus.carbon_footprint, 50) AS Carbon_Footprint, "
            f"COALESCE(sus.water_usage * 100, 300) AS Water_Footprint, "
            f"CURRENT_TIMESTAMP() AS timestamp, CURRENT_TIMESTAMP() AS created_at "
            f"FROM `{project_id}.{seed_dataset_id}.seed_farm` sf "
            f"JOIN `{project_id}.{dataset_id}.temp_synthetic_dates` d ON sf.farm_id = d.farm_id "
            f"JOIN `{project_id}.{seed_dataset_id}.seed_crop` sc ON d.crop_id = sc.crop_id "
            f"LEFT JOIN `{project_id}.{seed_dataset_id}.seed_harvest` sh ON sf.farm_id = sh.farm_id AND sc.crop_id = sh.crop_id "
            f"LEFT JOIN `{project_id}.{seed_dataset_id}.seed_yield` sy ON sh.harvest_id = sy.harvest_id "
            f"LEFT JOIN `{project_id}.{seed_dataset_id}.seed_sustainability` sus ON sf.farm_id = sus.farm_id "
            f"LEFT JOIN `{project_id}.{seed_dataset_id}.seed_soil` ss ON sf.farm_id = ss.farm_id "
            f"LIMIT 100"
        ], check=True)
        logger.info("Created full_data table")
        
        # Clean up temporary table
        logger.info("Cleaning up temporary table")
        subprocess.run([
            "bq", "rm", "-f", "-t", f"{project_id}:{dataset_id}.temp_synthetic_dates"
        ], check=True)
        logger.info("Cleaned up temporary table")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing bq command: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error creating OLAP tables: {str(e)}")
        raise

if __name__ == "__main__":
    main() 