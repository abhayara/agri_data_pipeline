#!/usr/bin/env python3

"""
GCS to BigQuery Export Script
This script exports data from GCS to BigQuery.
"""

import os
import logging
import subprocess
from google.cloud import storage
from google.cloud import bigquery
import datetime
import importlib.util
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main function to export data from GCS to BigQuery."""
    try:
        # Get environment variables
        gcs_bucket_name = os.environ.get('GCS_BUCKET_NAME', 'agri_data_tf_bucket')
        gcs_transformed_path = os.environ.get('GCS_TRANSFORMED_DATA_PATH', 'transformed/agri_data/')
        olap_path = os.environ.get('OLAP_DATA_PATH', 'olap/agri_data/')
        dataset_id = os.environ.get('GCP_DATASET_ID', 'agri_data')
        project_id = os.environ.get('GCP_PROJECT_ID', 'agri-data-454414')
        
        logger.info(f"Starting GCS to BigQuery export from gs://{gcs_bucket_name}/{gcs_transformed_path}")
        
        # Initialize clients
        storage_client = storage.Client()
        bigquery_client = bigquery.Client()
        
        # Create dataset if it doesn't exist
        dataset_ref = bigquery_client.dataset(dataset_id)
        try:
            bigquery_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_id} already exists")
        except Exception:
            logger.info(f"Creating dataset {dataset_id}")
            try:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "asia-south1"
                bigquery_client.create_dataset(dataset)
            except Exception as e:
                logger.warning(f"Could not create dataset: {str(e)}. Trying with bq command...")
                try:
                    # Try using bq command-line tool as fallback
                    subprocess.run(['bq', '--location=asia-south1', 'mk', '--dataset', f"{project_id}:{dataset_id}"], check=True)
                    logger.info(f"Created dataset {dataset_id} using bq command")
                except subprocess.CalledProcessError as e:
                    if 'already exists' in str(e):
                        logger.info(f"Dataset {dataset_id} already exists")
                    else:
                        logger.error(f"Failed to create dataset with bq command: {str(e)}")
                        raise
        
        # Process transformed data
        process_transformed_data(bigquery_client, storage_client, gcs_bucket_name, gcs_transformed_path, project_id, dataset_id)
        
        # Check for OLAP data in GCS
        olap_data_exists = check_olap_data_exists(storage_client, gcs_bucket_name, olap_path)
        
        if olap_data_exists:
            # Process OLAP data with special handling for partitioned tables
            logger.info("OLAP data found in GCS. Processing...")
            process_olap_data(bigquery_client, storage_client, gcs_bucket_name, olap_path, project_id, dataset_id)
        else:
            # Create OLAP tables from seed data if they don't exist in GCS
            logger.info("No OLAP data found in GCS. Creating OLAP tables from seed data...")
            create_olap_tables_from_seed(project_id, dataset_id)
        
        logger.info("GCS to BigQuery export completed successfully")
    except Exception as e:
        logger.error(f"Error in GCS to BigQuery export: {str(e)}")
        raise

def check_olap_data_exists(storage_client, gcs_bucket_name, olap_path):
    """Check if OLAP data exists in GCS."""
    try:
        # Check for farm_analysis, crop_analysis, and full_data directories
        required_paths = [
            f"{olap_path}farm_analysis",
            f"{olap_path}crop_analysis",
            f"{olap_path}full_data"
        ]
        
        bucket = storage_client.bucket(gcs_bucket_name)
        
        for path in required_paths:
            blobs = list(bucket.list_blobs(prefix=path))
            # Check for actual data files, not just directories
            if not any(not blob.name.endswith('/') for blob in blobs):
                logger.info(f"No data found in {path}")
                return False
        
        return True
    except Exception as e:
        logger.warning(f"Error checking for OLAP data: {str(e)}")
        return False

def create_olap_tables_from_seed(project_id, dataset_id):
    """Create OLAP tables from seed data."""
    try:
        # Import the create_olap_tables script
        script_path = os.path.join(os.path.dirname(__file__), 'create_olap_tables.py')
        if not os.path.exists(script_path):
            logger.error(f"Script not found: {script_path}")
            # Fall back to creating tables here if the script doesn't exist
            create_olap_tables_fallback(project_id, dataset_id)
            return
        
        # Run the script directly
        logger.info("Running create_olap_tables.py script...")
        result = subprocess.run([sys.executable, script_path], check=True)
        
        if result.returncode == 0:
            logger.info("Successfully created OLAP tables from seed data")
        else:
            logger.error("Failed to create OLAP tables from seed data")
            
    except Exception as e:
        logger.error(f"Error creating OLAP tables from seed data: {str(e)}")
        # Fall back to creating tables here if the script fails
        create_olap_tables_fallback(project_id, dataset_id)

def create_olap_tables_fallback(project_id, dataset_id):
    """Fallback method to create OLAP tables if the script approach fails."""
    seed_dataset_id = 'agri_data_seeds'
    logger.info("Using fallback method to create OLAP tables...")
    
    try:
        # Create farm_analysis
        subprocess.run([
            "bq", "query", "--use_legacy_sql=false", "--location=asia-south1",
            f"CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.farm_analysis` AS "
            f"SELECT sf.farm_id AS Farm_ID, 'Mixed' AS Farm_Type, sf.farm_size AS avg_Farm_Size_Acres, "
            f"60 AS avg_Soil_Moisture, 55 AS avg_Soil_Temperature "
            f"FROM `{project_id}.{seed_dataset_id}.seed_farm` sf"
        ], check=True)
        
        # Create crop_analysis
        subprocess.run([
            "bq", "query", "--use_legacy_sql=false", "--location=asia-south1",
            f"CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.crop_analysis` AS "
            f"SELECT sc.crop_type AS Crop_Type, sc.crop_name AS Crop_Variety, "
            f"100 AS avg_Expected_Yield, 90 AS avg_Actual_Yield, 90 AS avg_Growing_Period_Days "
            f"FROM `{project_id}.{seed_dataset_id}.seed_crop` sc"
        ], check=True)
        
        # Create full_data with a simpler approach
        subprocess.run([
            "bq", "query", "--use_legacy_sql=false", "--location=asia-south1",
            f"CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.full_data` AS "
            f"SELECT sf.farm_id AS Farm_ID, 'Mixed' AS Farm_Type, sf.farm_size AS Farm_Size_Acres, "
            f"CAST(sc.crop_id AS STRING) AS Crop_ID, CURRENT_DATE() AS Planting_Date, "
            f"CURRENT_DATE() AS Harvest_Date, 100 AS Expected_Yield, 90 AS Actual_Yield, "
            f"60 AS Soil_Moisture, 55 AS Soil_Temperature, 75 AS Sustainability_Score, "
            f"EXTRACT(YEAR FROM CURRENT_DATE()) AS year, EXTRACT(MONTH FROM CURRENT_DATE()) AS month, "
            f"sf.farm_location AS Farm_Location, sf.farm_name AS Farmer_Contact, 'kg/hectare' AS Yield_Unit, "
            f"6.5 AS Soil_pH, 'Medium' AS Soil_Fertility, 8 AS Weather_Sunlight, 200 AS Irrigation_Amount, "
            f"100 AS Fertilizer_Amount, 'Organic' AS Pesticide_Type, 30 AS Pesticide_Amount, "
            f"'Tractor' AS Equipment_Used, 200 AS Labor_Hours, 1000 AS Total_Revenue, 'B' AS Quality_Grade, "
            f"'Conventional' AS Certification, 50 AS Carbon_Footprint, 300 AS Water_Footprint, "
            f"CURRENT_TIMESTAMP() AS timestamp, CURRENT_TIMESTAMP() AS created_at "
            f"FROM `{project_id}.{seed_dataset_id}.seed_farm` sf "
            f"CROSS JOIN `{project_id}.{seed_dataset_id}.seed_crop` sc LIMIT 100"
        ], check=True)
        
        logger.info("Successfully created OLAP tables using fallback method")
    except Exception as e:
        logger.error(f"Error in fallback OLAP table creation: {str(e)}")
        raise

def process_transformed_data(bigquery_client, storage_client, gcs_bucket_name, gcs_transformed_path, project_id, dataset_id):
    """Process and load transformed data to BigQuery."""
    # List files in the GCS path
    bucket = storage_client.bucket(gcs_bucket_name)
    blobs = list(bucket.list_blobs(prefix=gcs_transformed_path))
    
    if not blobs:
        logger.info("No transformed files found in GCS path. Nothing to export.")
        return
    
    logger.info(f"Found {len(blobs)} transformed files to process")
    
    # Process each file
    for blob in blobs:
        if blob.name.endswith('/'):  # Skip directory markers
            continue
            
        table_name = os.path.basename(blob.name).split('.')[0]
        
        # Create load job configuration
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        # Uri for the GCS file
        uri = f"gs://{gcs_bucket_name}/{blob.name}"
        
        # Load the data into BigQuery
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        logger.info(f"Loading transformed data into {table_id} from {uri}")
        
        try:
            load_job = bigquery_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            
            # Wait for the job to complete
            load_job.result()
            
            logger.info(f"Loaded {load_job.output_rows} rows into {table_id}")
        except Exception as e:
            logger.error(f"Error loading transformed data into {table_id}: {str(e)}")
            try:
                # Try using bq command-line tool as fallback
                load_cmd = [
                    'bq', 'load', 
                    '--location=asia-south1', 
                    '--source_format=PARQUET', 
                    '--autodetect', 
                    f"{project_id}:{dataset_id}.{table_name}", 
                    uri
                ]
                logger.info(f"Trying with bq command: {' '.join(load_cmd)}")
                subprocess.run(load_cmd, check=True)
                logger.info(f"Loaded data into {table_id} using bq command")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to load with bq command: {str(e)}")

def process_olap_data(bigquery_client, storage_client, gcs_bucket_name, olap_path, project_id, dataset_id):
    """Process and load OLAP data to BigQuery with special handling for partitioned tables."""
    # List directories in the OLAP path
    bucket = storage_client.bucket(gcs_bucket_name)
    
    # Check for farm_analysis
    farm_analysis_path = f"{olap_path}farm_analysis"
    process_simple_table(bigquery_client, storage_client, gcs_bucket_name, farm_analysis_path, 
                        "farm_analysis", project_id, dataset_id)
    
    # Check for crop_analysis
    crop_analysis_path = f"{olap_path}crop_analysis"
    process_simple_table(bigquery_client, storage_client, gcs_bucket_name, crop_analysis_path, 
                        "crop_analysis", project_id, dataset_id)
    
    # Special handling for full_data which is partitioned
    full_data_path = f"{olap_path}full_data"
    process_partitioned_table(bigquery_client, storage_client, gcs_bucket_name, full_data_path, 
                             "full_data", project_id, dataset_id)

def process_simple_table(bigquery_client, storage_client, gcs_bucket_name, path, table_name, project_id, dataset_id):
    """Process and load a simple (non-partitioned) table to BigQuery."""
    bucket = storage_client.bucket(gcs_bucket_name)
    blobs = list(bucket.list_blobs(prefix=path))
    
    if not blobs or all(blob.name.endswith('/') for blob in blobs):
        logger.info(f"No files found in {path}. Skipping {table_name}.")
        return
    
    # Create load job configuration
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    # Uri for the GCS path
    uri = f"gs://{gcs_bucket_name}/{path}/*.parquet"
    
    # Load the data into BigQuery
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    logger.info(f"Loading OLAP data into {table_id} from {uri}")
    
    try:
        load_job = bigquery_client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        
        # Wait for the job to complete
        load_job.result()
        
        logger.info(f"Loaded data into {table_id}")
    except Exception as e:
        logger.error(f"Error loading {table_name}: {str(e)}")
        try:
            # Try using bq command-line tool as fallback
            load_cmd = [
                'bq', 'load', 
                '--location=asia-south1', 
                '--source_format=PARQUET', 
                '--autodetect', 
                '--replace',
                f"{project_id}:{dataset_id}.{table_name}", 
                uri
            ]
            logger.info(f"Trying with bq command: {' '.join(load_cmd)}")
            subprocess.run(load_cmd, check=True)
            logger.info(f"Loaded data into {table_id} using bq command")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to load with bq command: {str(e)}")

def process_partitioned_table(bigquery_client, storage_client, gcs_bucket_name, path, table_name, project_id, dataset_id):
    """Process and load a partitioned table to BigQuery with time partitioning."""
    bucket = storage_client.bucket(gcs_bucket_name)
    
    # Check if the path exists and contains data
    blobs = list(bucket.list_blobs(prefix=path, delimiter='/'))
    if not any(not blob.name.endswith('/') for blob in blobs):
        logger.info(f"No data found in {path}. Skipping {table_name}.")
        return
    
    # Create partitioned table schema - using autodetect but specifying partitioning
    
    # Check if table exists and delete if it does
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    try:
        bigquery_client.delete_table(table_id)
        logger.info(f"Deleted existing table {table_id}")
    except Exception:
        logger.info(f"Table {table_id} does not exist yet")
    
    # Create load job configuration with partitioning
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="Planting_Date"
        ),
        clustering_fields=["Farm_ID", "Crop_ID"],
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ]
    )
    
    # First, check the partition structure by listing directories
    prefix = f"{path}/"
    blobs = list(bucket.list_blobs(prefix=prefix, delimiter='/'))
    directories = [blob.name for blob in blobs if blob.name.endswith('/')]
    
    # If no year/month structure is found, load directly
    if not directories or not any('year=' in d for d in directories):
        uri = f"gs://{gcs_bucket_name}/{path}/*.parquet"
        logger.info(f"No partitioning found. Loading data directly from {uri}")
        
        try:
            load_job = bigquery_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            load_job.result()
            logger.info(f"Loaded non-partitioned data into {table_id}")
            return
        except Exception as e:
            logger.error(f"Error loading non-partitioned data: {str(e)}")
            try:
                # Try using bq command-line tool as fallback
                load_cmd = [
                    'bq', 'load', 
                    '--location=asia-south1', 
                    '--source_format=PARQUET', 
                    '--autodetect', 
                    '--time_partitioning_field=Planting_Date',
                    '--clustering_fields=Farm_ID,Crop_ID',
                    f"{project_id}:{dataset_id}.{table_name}", 
                    uri
                ]
                logger.info(f"Trying with bq command: {' '.join(load_cmd)}")
                subprocess.run(load_cmd, check=True)
                logger.info(f"Loaded non-partitioned data into {table_id} using bq command")
                return
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to load with bq command: {str(e)}")
                return
    
    # If partitioning exists, set up Hive partitioning options
    job_config.hive_partitioning_options = bigquery.HivePartitioningOptions(
        mode="AUTO",
        source_uri_prefix=f"gs://{gcs_bucket_name}/{path}/"
    )
    
    # Uri for partitioned data
    uri = f"gs://{gcs_bucket_name}/{path}/*/*.parquet"
    
    logger.info(f"Loading partitioned data into {table_id} from {uri}")
    
    try:
        load_job = bigquery_client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        
        # Wait for the job to complete
        load_job.result()
        
        logger.info(f"Loaded partitioned data into {table_id}")
    except Exception as e:
        logger.error(f"Error loading partitioned data: {str(e)}")
        # Attempt to load without partitioning as fallback
        logger.info("Attempting to load without partitioning as fallback...")
        job_config.hive_partitioning_options = None
        uri = f"gs://{gcs_bucket_name}/{path}/*.parquet"
        
        try:
            load_job = bigquery_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            load_job.result()
            logger.info(f"Loaded data without partitioning into {table_id}")
        except Exception as e2:
            logger.error(f"Fallback loading also failed: {str(e2)}")
            try:
                # Try using bq command-line tool as fallback
                load_cmd = [
                    'bq', 'load', 
                    '--location=asia-south1', 
                    '--source_format=PARQUET', 
                    '--autodetect', 
                    '--time_partitioning_field=Planting_Date',
                    '--clustering_fields=Farm_ID,Crop_ID',
                    f"{project_id}:{dataset_id}.{table_name}", 
                    uri
                ]
                logger.info(f"Trying with bq command: {' '.join(load_cmd)}")
                subprocess.run(load_cmd, check=True)
                logger.info(f"Loaded data into {table_id} using bq command")
            except subprocess.CalledProcessError as e3:
                logger.error(f"Failed to load with bq command: {str(e3)}")

if __name__ == "__main__":
    main() 