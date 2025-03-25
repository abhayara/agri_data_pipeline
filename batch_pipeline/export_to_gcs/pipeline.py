"""
Spark pipeline for processing agricultural data.
This script performs OLAP transformations on agricultural data and stores the results in GCS.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, avg, sum, count, max, min, to_date, year, month, dayofmonth, lit
import config
from utils import setup_spark_with_gcs_connector

def create_spark_session():
    """Create and configure a Spark session with GCS connector."""
    spark = setup_spark_with_gcs_connector()
    return spark

def load_raw_data(spark, bucket, path):
    """Load raw agricultural data from GCS."""
    try:
        # Read all CSV files in the specified GCS path
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"gs://{bucket}/{path}*")
        return df
    except Exception as e:
        print(f"Error loading raw data: {e}")
        sys.exit(1)

def transform_farm_dimension(df):
    """Transform and create farm dimension table."""
    farm_dim = df.select(
        col("Farm_ID").alias("farm_id"),
        col("Farm_Type").alias("farm_type"),
        col("Farm_Size_Acres").alias("size_acres"),
        col("Farm_Location").alias("location"),
        col("Farmer_ID").alias("farmer_id"),
        col("Farmer_Name").alias("farmer_name"),
        col("Farmer_Contact").alias("farmer_contact")
    ).dropDuplicates(["farm_id"])
    
    return farm_dim

def transform_crop_dimension(df):
    """Transform and create crop dimension table."""
    crop_dim = df.select(
        col("Crop_ID").alias("crop_id"),
        col("Crop_Type").alias("crop_type"),
        col("Crop_Variety").alias("crop_variety"),
        to_date(col("Planting_Date")).alias("planting_date"),
        to_date(col("Harvest_Date")).alias("harvest_date")
    ).dropDuplicates(["crop_id"])
    
    return crop_dim

def transform_weather_dimension(df):
    """Transform and create weather dimension table."""
    weather_dim = df.select(
        col("Farm_ID").alias("farm_id"),
        col("Weather_Temperature").alias("temperature"),
        col("Weather_Humidity").alias("humidity"),
        col("Weather_Rainfall").alias("rainfall"),
        col("Weather_Sunlight").alias("sunlight_hours")
    ).dropDuplicates(["farm_id"])
    
    return weather_dim

def transform_soil_dimension(df):
    """Transform and create soil dimension table."""
    soil_dim = df.select(
        col("Farm_ID").alias("farm_id"),
        col("Soil_Type").alias("soil_type"),
        col("Soil_pH").alias("ph"),
        col("Soil_Moisture").alias("moisture"),
        col("Soil_Temperature").alias("temperature"),
        col("Soil_Fertility").alias("fertility")
    ).dropDuplicates(["farm_id"])
    
    return soil_dim

def transform_harvest_dimension(df):
    """Transform and create harvest dimension table."""
    harvest_dim = df.select(
        col("Farm_ID").alias("farm_id"),
        col("Crop_ID").alias("crop_id"),
        to_date(col("Harvest_Date")).alias("harvest_date"),
        col("Expected_Yield").alias("expected_yield"),
        col("Actual_Yield").alias("actual_yield"),
        col("Yield_Unit").alias("yield_unit")
    ).dropDuplicates(["farm_id", "crop_id", "harvest_date"])
    
    return harvest_dim

def transform_production_facts(df):
    """Transform and create production facts table."""
    production_facts = df.select(
        col("Farm_ID").alias("farm_id"),
        col("Crop_ID").alias("crop_id"),
        to_date(col("Harvest_Date")).alias("harvest_date"),
        col("Production_Cost").alias("production_cost"),
        col("Market_Price").alias("market_price"),
        col("Total_Revenue").alias("total_revenue"),
        col("Profit_Margin").alias("profit_margin"),
        col("Labor_Hours").alias("labor_hours")
    )
    
    return production_facts

def transform_yield_facts(df):
    """Transform and create yield facts table."""
    yield_facts = df.select(
        col("Farm_ID").alias("farm_id"),
        col("Crop_ID").alias("crop_id"),
        to_date(col("Harvest_Date")).alias("harvest_date"),
        col("Expected_Yield").alias("expected_yield"),
        col("Actual_Yield").alias("actual_yield"),
        col("Yield_Unit").alias("yield_unit"),
        col("Irrigation_Amount").alias("irrigation_amount"),
        col("Fertilizer_Amount").alias("fertilizer_amount"),
        col("Pesticide_Amount").alias("pesticide_amount")
    )
    
    return yield_facts

def transform_sustainability_facts(df):
    """Transform and create sustainability facts table."""
    sustainability_facts = df.select(
        col("Farm_ID").alias("farm_id"),
        col("Crop_ID").alias("crop_id"),
        to_date(col("Harvest_Date")).alias("harvest_date"),
        col("Carbon_Footprint").alias("carbon_footprint"),
        col("Water_Footprint").alias("water_footprint"),
        col("Sustainability_Score").alias("sustainability_score"),
        col("Certification").alias("certification")
    )
    
    return sustainability_facts

def save_to_gcs(df, table_name, bucket, path):
    """Save transformed data to GCS in Parquet format."""
    output_path = f"gs://{bucket}/{path}{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Successfully saved {table_name} to {output_path}")

def main():
    """Main function to execute the pipeline."""
    spark = create_spark_session()
    
    # Load raw data
    raw_data = load_raw_data(spark, config.GCS_BUCKET, config.RAW_DATA_PATH)
    
    # Transform dimension tables
    farm_dimension = transform_farm_dimension(raw_data)
    crop_dimension = transform_crop_dimension(raw_data)
    weather_dimension = transform_weather_dimension(raw_data)
    soil_dimension = transform_soil_dimension(raw_data)
    harvest_dimension = transform_harvest_dimension(raw_data)
    
    # Transform fact tables
    production_facts = transform_production_facts(raw_data)
    yield_facts = transform_yield_facts(raw_data)
    sustainability_facts = transform_sustainability_facts(raw_data)
    
    # Save dimension tables to GCS
    save_to_gcs(farm_dimension, "farm_dimension", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    save_to_gcs(crop_dimension, "crop_dimension", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    save_to_gcs(weather_dimension, "weather_dimension", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    save_to_gcs(soil_dimension, "soil_dimension", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    save_to_gcs(harvest_dimension, "harvest_dimension", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    
    # Save fact tables to GCS
    save_to_gcs(production_facts, "production_facts", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    save_to_gcs(yield_facts, "yield_facts", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    save_to_gcs(sustainability_facts, "sustainability_facts", config.GCS_BUCKET, config.TRANSFORMED_DATA_PATH)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
