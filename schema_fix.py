#!/usr/bin/env python3
# Batch processing script that handles schema mismatches

import os
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth

def main():
    """Main entry point for batch processing with schema handling"""
    print("Starting Agricultural Data Batch Processing with Schema Handling")
    
    # Get environment variables
    bucket_name = os.environ.get("GCS_BUCKET_NAME", "agri_data_tf_bucket")
    input_path = os.environ.get("GCS_RAW_DATA_PATH", "raw/agri_data")
    output_path = os.environ.get("GCS_OLAP_DATA_PATH", "olap/agri_data")
    
    print(f"Configuration - Bucket: {bucket_name}, Input: {input_path}, Output: {output_path}")
    
    # Create Spark session
    print("Initializing Spark session with GCS connector...")
    
    # Create a simple Spark session
    spark = (SparkSession.builder
        .appName("AgriculturalDataBatchProcessing")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/opt/spark-apps/gcp-creds.json")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/spark-apps/gcp-creds.json")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/opt/spark-apps/gcp-creds.json")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/spark-apps/gcp-creds.json")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.legacy.parquet.int64AsTimestamp", "false")
        .getOrCreate())
    
    print("✅ Spark session initialized successfully")
    
    try:
        # Set full GCS paths
        input_gcs_path = f"gs://{bucket_name}/{input_path}"
        output_gcs_path = f"gs://{bucket_name}/{output_path}"
        
        print(f"Reading data from: {input_gcs_path}")
        
        # Instead of using mergeSchema, we'll read each file individually and convert types
        file_list = spark._jvm.org.apache.hadoop.fs.Path(input_gcs_path)
        fs = file_list.getFileSystem(spark._jsc.hadoopConfiguration())
        parquet_files = [f.getPath().toString() 
                         for f in fs.listStatus(file_list) 
                         if f.getPath().getName().endswith(".parquet")]
        
        print(f"Found {len(parquet_files)} parquet files")
        
        # Process each file individually
        dfs = []
        for file_path in parquet_files:
            print(f"Processing file: {file_path}")
            df = spark.read.parquet(file_path)
            
            # Ensure columns have consistent types
            if "Pesticide_Amount" in df.columns:
                df = df.withColumn("Pesticide_Amount", col("Pesticide_Amount").cast("double"))
                print(f"Converted Pesticide_Amount to double")
                
            if "Irrigation_Amount" in df.columns:
                df = df.withColumn("Irrigation_Amount", col("Irrigation_Amount").cast("double"))
                print(f"Converted Irrigation_Amount to double")
                
            dfs.append(df)
        
        # Union all dataframes
        if dfs:
            print("Combining all dataframes")
            df = dfs[0]
            for other_df in dfs[1:]:
                df = df.unionByName(other_df, allowMissingColumns=True)
            
            # Fill null values that might have occurred due to unionByName
            df = df.na.fill(0, ["Pesticide_Amount", "Irrigation_Amount"])
            
            # Print schema and sample data
            print("Final schema:")
            df.printSchema()
            
            # Print a few rows to verify the data
            print("Printing 2 rows to verify the data:")
            df.limit(2).show(truncate=False)
            
            print(f"Read {df.count()} total records from GCS")
            
            # Perform transformations
            print("Applying transformations...")
            
            # Convert timestamp to date and extract components
            if "timestamp" in df.columns:
                df = df.withColumn("date", to_date(col("timestamp")))
                df = df.withColumn("year", year(col("date")))
                df = df.withColumn("month", month(col("date")))
                df = df.withColumn("day", dayofmonth(col("date")))
            
            # Create a farm analytical view
            farm_analysis = df.groupBy("Farm_ID", "Farm_Type").agg(
                {"Farm_Size_Acres": "avg", "Soil_Moisture": "avg", "Soil_Temperature": "avg"}
            )
            
            # Create a crop analytical view
            crop_analysis = df.groupBy("Crop_Type", "Crop_Variety").agg(
                {"Expected_Yield": "avg", "Actual_Yield": "avg", "Growing_Period_Days": "avg"}
            )
            
            # Write transformed data to GCS
            print(f"Writing transformed data to: {output_gcs_path}")
            
            # Write farm analysis
            farm_analysis_path = f"{output_gcs_path}/farm_analysis"
            farm_analysis.write.mode("overwrite").parquet(farm_analysis_path)
            
            # Write crop analysis
            crop_analysis_path = f"{output_gcs_path}/crop_analysis"
            crop_analysis.write.mode("overwrite").parquet(crop_analysis_path)
            
            # Write full processed dataset
            full_data_path = f"{output_gcs_path}/full_data"
            df.write.mode("overwrite").partitionBy("year", "month").parquet(full_data_path)
            
            print("✅ Batch processing completed successfully")
            
            # Log success
            with open("/opt/spark-apps/batch_completion.log", "a") as f:
                f.write(f"Batch processing completed at {os.popen('date').read().strip()}\n")
            
            return True
        else:
            print("❌ No files found to process")
            return False
            
    except Exception as e:
        print(f"❌ Error in batch processing: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 