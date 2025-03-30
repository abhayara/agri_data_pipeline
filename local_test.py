#!/usr/bin/env python3
# Simple test script to read a local parquet file

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth

def main():
    """Main entry point for local file test"""
    print("Starting Local Parquet File Test")
    
    # Create a simple Spark session
    spark = (SparkSession.builder
        .appName("LocalParquetTest")
        .getOrCreate())
    
    # Path to local file
    local_file = "/tmp/agri_data_batch_1b55437a_20250330_142926.parquet"
    
    print(f"Reading local parquet file: {local_file}")
    
    try:
        # Read the local parquet file
        df = spark.read.parquet(local_file)
        
        # Print schema and sample data
        print("Data schema:")
        df.printSchema()
        
        print("Sample data:")
        df.show(5, truncate=False)
        
        print(f"Successfully read {df.count()} records from local file")
        
        # Try to write a test OLAP file locally
        print("Writing test OLAP file locally...")
        
        # Create a farm analytical view
        farm_analysis = df.groupBy("Farm_ID", "Farm_Type").agg(
            {"Farm_Size_Acres": "avg", "Soil_Moisture": "avg", "Soil_Temperature": "avg"}
        )
        
        # Write farm analysis
        test_output_path = "/tmp/farm_analysis_test"
        farm_analysis.write.mode("overwrite").parquet(test_output_path)
        
        print(f"Successfully wrote test OLAP file to {test_output_path}")
        
        # Now try to write to GCS
        print("Now attempting to write to GCS...")
        bucket_name = "agri_data_tf_bucket"
        output_path = "olap/agri_data/test"
        gcs_output_path = f"gs://{bucket_name}/{output_path}"
        
        try:
            farm_analysis.write.mode("overwrite").parquet(gcs_output_path)
            print(f"Successfully wrote to GCS path: {gcs_output_path}")
        except Exception as e:
            print(f"Error writing to GCS: {str(e)}")
            import traceback
            traceback.print_exc()
    
    except Exception as e:
        print(f"Error in local file test: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("Local file test completed.")
    spark.stop()

if __name__ == "__main__":
    main() 