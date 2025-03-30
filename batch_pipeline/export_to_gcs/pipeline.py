"""
Spark pipeline for processing agricultural data.
This script performs transformations on agricultural data and stores the results in GCS.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from dotenv import load_dotenv
import config
from utils import setup_spark_with_gcs_connector, check_gcs_bucket_exists, list_gcs_files

# Load environment variables
load_dotenv()

def create_spark_session():
    """Create and configure a Spark session with GCS connector."""
    spark = setup_spark_with_gcs_connector()
    return spark

def load_raw_data(spark, bucket, path):
    """Load raw agricultural data from GCS."""
    try:
        # Check for files
        files = list_gcs_files(bucket, path)
        if not files:
            print("No raw data files found")
            return None
            
        # Read JSON files in the specified GCS path
        df = spark.read.json(f"gs://{bucket}/{path}*")
        print(f"Loaded {df.count()} rows of raw data")
        return df
    except Exception as e:
        print(f"Error loading raw data: {e}")
        return None

def transform_farm_dimension(df):
    """Transform and create farm dimension table."""
    farm_dim = df.select(
        col("farm_id"),
        col("farm_name"),
        col("farm_location"),
        col("farm_size_acres")
    ).dropDuplicates(["farm_id"])
    
    return farm_dim

def transform_crop_dimension(df):
    """Transform and create crop dimension table."""
    crop_dim = df.select(
        col("crop_id"),
        col("crop_type"),
        to_date(col("planting_date")).alias("planting_date"),
        to_date(col("harvest_date")).alias("harvest_date")
    ).dropDuplicates(["crop_id"])
    
    return crop_dim

def transform_weather_dimension(df):
    """Transform and create weather dimension table."""
    weather_dim = df.select(
        col("farm_id"),
        col("weather_temperature").alias("temperature"),
        col("weather_humidity").alias("humidity"),
        col("weather_rainfall").alias("rainfall")
    ).dropDuplicates(["farm_id"])
    
    return weather_dim

def transform_soil_dimension(df):
    """Transform and create soil dimension table."""
    soil_dim = df.select(
        col("farm_id"),
        col("soil_type"),
        col("soil_ph").alias("ph")
    ).dropDuplicates(["farm_id"])
    
    return soil_dim

def transform_yield_facts(df):
    """Transform and create yield facts table."""
    yield_facts = df.select(
        col("farm_id"),
        col("crop_id"),
        to_date(col("harvest_date")).alias("harvest_date"),
        col("expected_yield"),
        col("actual_yield"),
        col("yield_unit")
    )
    
    return yield_facts

def transform_sustainability_facts(df):
    """Transform and create sustainability facts table."""
    sustainability_facts = df.select(
        col("farm_id"),
        col("crop_id"),
        to_date(col("harvest_date")).alias("harvest_date"),
        col("sustainability_score")
    )
    
    return sustainability_facts

def save_to_gcs(df, table_name, bucket, path):
    """Save transformed data to GCS in Parquet format."""
    if df is None or df.count() == 0:
        print(f"No data to save for {table_name}")
        return
        
    output_path = f"gs://{bucket}/{path}{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved {df.count()} rows to {output_path}")

def main():
    """Main function to execute the pipeline."""
    # Ensure bucket exists
    if not check_gcs_bucket_exists(config.GCS_BUCKET):
        print("Failed to verify GCS bucket. Exiting.")
        sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Load raw data
    raw_data = load_raw_data(spark, config.GCS_BUCKET, config.RAW_DATA_PATH)
    if raw_data is None:
        print("No raw data to process. Exiting.")
        spark.stop()
        sys.exit(1)
    
    # Transform dimension tables
    farm_dimension = transform_farm_dimension(raw_data)
    crop_dimension = transform_crop_dimension(raw_data)
    weather_dimension = transform_weather_dimension(raw_data)
    soil_dimension = transform_soil_dimension(raw_data)
    
    # Transform fact tables
    yield_facts = transform_yield_facts(raw_data)
    sustainability_facts = transform_sustainability_facts(raw_data)
    
    # Save dimension tables to GCS
    save_to_gcs(farm_dimension, "farm_dimension", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    save_to_gcs(crop_dimension, "crop_dimension", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    save_to_gcs(weather_dimension, "weather_dimension", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    save_to_gcs(soil_dimension, "soil_dimension", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    
    # Save fact tables to GCS
    save_to_gcs(yield_facts, "yield_facts", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    save_to_gcs(sustainability_facts, "sustainability_facts", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    
    # Stop Spark session
    spark.stop()
    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
