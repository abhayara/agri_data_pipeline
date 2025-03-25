"""
Utility functions for the agricultural data batch pipeline.
"""

import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

def setup_spark_with_gcs_connector():
    """
    Set up and configure a Spark session with GCS connector.
    
    Returns:
        SparkSession: A configured Spark session
    """
    # Get credentials path
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', './gcp-creds.json')
    
    # Path to the GCS connector jar file
    jar_file_path = "./docker/spark/jar_files/gcs-connector-hadoop3-2.2.5.jar"
    
    # Project ID
    project_id = os.getenv('GCP_PROJECT_ID', 'agri-data-454414')
    
    # Spark configuration
    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('AgriDataTransformation') \
        .set("spark.jars", jar_file_path) \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_path) \
        .set("spark.hadoop.google.cloud.project.id", project_id)
    
    # Create Spark Context
    sc = SparkContext(conf=conf)
    
    # Hadoop configuration for GCS access
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_path)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    
    # Create Spark Session
    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()
    
    return spark

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

