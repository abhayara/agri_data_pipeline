#!/bin/bash
set -e

echo "Running final batch processing with verified settings..."

# Kill any existing batch processing jobs
docker exec agri_data_pipeline-spark-master pkill -f batch_processing.py || true

# Update the batch processing script
cat > /tmp/batch_processing_fixed.py << 'EOL'
#!/usr/bin/env python3
# Batch processing script for agricultural data

import os
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth

def main():
    """Main entry point for batch processing"""
    print("Starting Agricultural Data Batch Processing")
    
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
        .getOrCreate())
    
    print("✅ Spark session initialized successfully")
    
    try:
        # Set full GCS paths
        input_gcs_path = f"gs://{bucket_name}/{input_path}"
        output_gcs_path = f"gs://{bucket_name}/{output_path}"
        
        print(f"Reading data from: {input_gcs_path}")
        
        # Read raw data from GCS
        df = spark.read.parquet(input_gcs_path)
        
        # Print schema and sample data
        print("Data schema:")
        df.printSchema()
        
        print("Sample data:")
        df.show(5, truncate=False)
        
        print(f"Read {df.count()} records from GCS")
        
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
        
    except Exception as e:
        print(f"❌ Error in batch processing: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
EOL

# Copy the batch processing script to the container
echo "Copying the fixed batch processing script to the container..."
docker cp /tmp/batch_processing_fixed.py agri_data_pipeline-spark-master:/opt/spark-apps/batch_processing_fixed.py
docker exec agri_data_pipeline-spark-master chmod +x /opt/spark-apps/batch_processing_fixed.py

# Run the batch processing job directly (using local mode for simplicity)
echo "Running batch processing job..."
docker exec agri_data_pipeline-spark-master /usr/bin/spark-3.3.1-bin-hadoop3/bin/spark-submit \
  --master local[*] \
  --verbose \
  --jars /usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11-shaded.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-cloud-storage-2.27.1.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/guava-31.1-jre.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-api-client-2.2.0.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-http-client-1.42.3.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-oauth2-http-1.19.0.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-credentials-1.19.0.jar \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.gs.auth.service.account.enable=true \
  --conf spark.hadoop.fs.gs.auth.service.account.json.keyfile=/opt/spark-apps/gcp-creds.json \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/spark-apps/gcp-creds.json \
  /opt/spark-apps/batch_processing_fixed.py

echo "Job completed. Check GCS for OLAP data." 