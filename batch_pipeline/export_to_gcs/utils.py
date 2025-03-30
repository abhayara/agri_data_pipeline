"""
Utility functions for the agricultural data batch pipeline.
"""

import os
import sys
from pyspark.sql import SparkSession
from google.cloud import storage
from dotenv import load_dotenv
import config

# Load environment variables
load_dotenv()

def setup_spark_with_gcs_connector():
    """
    Set up a Spark session with the necessary configurations for GCS connectivity.
    """
    # Get GCP configuration from environment
    project_id = os.getenv('GCP_PROJECT_ID', 'agri-data-454414')
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName('AgriDataTransformation') \
        .config('spark.jars', '/opt/spark/jars/gcs-connector-hadoop3-latest.jar') \
        .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
        .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', '/opt/spark/conf/gcp-creds.json') \
        .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
        .config('spark.hadoop.fs.gs.project.id', project_id) \
        .getOrCreate()
    
    print("Spark session created with GCS connector configured")
    return spark

def check_gcs_bucket_exists(bucket_name):
    """
    Check if the GCS bucket exists, create it if it doesn't.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        if not bucket.exists():
            print(f"Bucket {bucket_name} does not exist, creating it...")
            storage_client.create_bucket(bucket_name)
            print(f"Created bucket {bucket_name}")
        else:
            print(f"Bucket {bucket_name} exists")
        
        return True
    except Exception as e:
        print(f"Error checking/creating GCS bucket: {e}")
        return False

def list_gcs_files(bucket_name, prefix):
    """
    List files in a GCS bucket with a given prefix.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            print(f"No files found in gs://{bucket_name}/{prefix}")
            return []
        
        files = [blob.name for blob in blobs]
        print(f"Found {len(files)} files in gs://{bucket_name}/{prefix}")
        return files
    except Exception as e:
        print(f"Error listing GCS files: {e}")
        return []

# Schema definitions for validation (optional)
farm_schema = {
    "farm_id": "string", 
    "farm_type": "string", 
    "size_acres": "double", 
    "location": "string",
    "farmer_id": "string",
    "farmer_name": "string",
    "farmer_contact": "string"
}

crop_schema = {
    "crop_id": "string",
    "crop_type": "string",
    "crop_variety": "string",
    "planting_date": "date",
    "harvest_date": "date"
}

weather_schema = {
    "farm_id": "string",
    "temperature": "double",
    "humidity": "double",
    "rainfall": "double",
    "sunlight_hours": "double"
}

soil_schema = {
    "farm_id": "string",
    "soil_type": "string",
    "ph": "double",
    "moisture": "double",
    "temperature": "double",
    "fertility": "integer"
}

harvest_schema = {
    "farm_id": "string",
    "crop_id": "string",
    "harvest_date": "date",
    "expected_yield": "double",
    "actual_yield": "double",
    "yield_unit": "string"
}

production_schema = {
    "farm_id": "string",
    "crop_id": "string",
    "harvest_date": "date",
    "production_cost": "double",
    "market_price": "double",
    "total_revenue": "double",
    "profit_margin": "double",
    "labor_hours": "double"
}

yield_schema = {
    "farm_id": "string",
    "crop_id": "string",
    "harvest_date": "date",
    "expected_yield": "double",
    "actual_yield": "double",
    "yield_unit": "string",
    "irrigation_amount": "double",
    "fertilizer_amount": "double",
    "pesticide_amount": "double"
}

