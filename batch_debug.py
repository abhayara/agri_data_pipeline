#!/usr/bin/env python3
# Debug script for batch processing to diagnose GCS connectivity issues

import os
import sys
import traceback
from pyspark.sql import SparkSession

def main():
    """Main entry point for batch debugging"""
    print("Starting GCS Connectivity Debug Script")
    
    # Set environment variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/opt/spark-apps/gcp-creds.json"
    
    print("DEBUGGING: Environment variables:")
    for key, value in os.environ.items():
        if key.startswith("HADOOP") or key.startswith("GOOGLE") or "GCS" in key:
            print(f"  {key}: {value}")
    
    print("\nDEBUGGING: Creating Spark session with GCS connector...")
    
    # Create a basic spark session for troubleshooting
    spark = (SparkSession.builder
        .appName("GCSConnectivityDebug")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/opt/spark-apps/gcp-creds.json")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/spark-apps/gcp-creds.json")
        .getOrCreate())
    
    print("\nDEBUGGING: Spark session created successfully")
    
    # Get configuration
    bucket_name = "agri_data_tf_bucket"
    input_path = "raw/agri_data"
    full_path = f"gs://{bucket_name}/{input_path}"
    
    print(f"\nDEBUGGING: Attempting to list files at path: {full_path}")
    
    try:
        # Try to list files from GCS
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem
        hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
        
        # Print hadoop configuration
        print("\nDEBUGGING: Hadoop configuration:")
        conf_keys = []
        iterator = hadoop_conf.iterator()
        while iterator.hasNext():
            entry = iterator.next()
            key = entry.getKey()
            if "gs" in key or "google" in key:
                conf_keys.append(key)
                print(f"  {key}: {hadoop_conf.get(key)}")
        
        # Try to access GCS directly with hadoop FileSystem
        fs = hadoop_fs.get(hadoop_conf)
        print(f"\nDEBUGGING: FileSystem scheme: {fs.getScheme()}")
        
        # Try to list files in GCS
        path = hadoop_path(full_path)
        status = fs.listStatus(path)
        
        print("\nDEBUGGING: Files found:")
        for file_status in status:
            print(f"  {file_status.getPath()}")
        
        # Try to read from GCS using Spark
        print("\nDEBUGGING: Trying to read files with Spark...")
        
        try:
            df = spark.read.parquet(full_path)
            print(f"Successfully read {df.count()} records")
            print("Schema:")
            df.printSchema()
            print("Sample data:")
            df.show(5, truncate=False)
        except Exception as e:
            print(f"Error reading files with Spark: {str(e)}")
            traceback.print_exc()
    
    except Exception as e:
        print(f"\nDEBUGGING: Error accessing GCS: {str(e)}")
        traceback.print_exc()
    
    print("\nDEBUGGING: Debug script completed.")
    spark.stop()

if __name__ == "__main__":
    main() 