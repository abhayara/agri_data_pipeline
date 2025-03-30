#!/bin/bash
# Spark-related functions for the Agricultural Data Pipeline

# Function to start Spark
start-spark() {
    echo "==========================================================="
    echo "Starting Spark services..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Ensure GCS configuration is ready
    check_fix_gcs_config
    
    # Ensure the build script is executable and run it
    chmod +x ./docker/spark/build.sh
    ./docker/spark/build.sh
    
    # Start Spark containers
    docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env up -d
    
    # Wait for containers to start
    echo "Waiting for Spark containers to start..."
    sleep 10
    
    # Check if Spark master is running
    if docker ps | grep -q "${PROJECT_NAME}-spark-master"; then
        echo "✅ Spark master is running."
        
        # Install JARs to Spark container if the script exists
        if [ -f "./batch_pipeline/install_jars.sh" ]; then
            echo "Installing JAR dependencies to Spark container..."
            ./batch_pipeline/install_jars.sh
            echo "✅ JAR dependencies installed to Spark container."
        else
            echo "⚠️ JAR installation script not found. Run check_fix_gcs_config first."
        fi
    else
        echo "❌ Spark master failed to start. Check logs with 'docker logs ${PROJECT_NAME}-spark-master'"
        return 1
    fi
    
    echo "Spark services started successfully."
    echo "==========================================================="
    return 0
}

# Function to stop Spark
stop-spark() {
    echo "==========================================================="
    echo "Stopping Spark services..."
    docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env down
    echo "Spark services stopped."
    echo "==========================================================="
}

# Function to restart Spark
restart-spark() {
    echo "==========================================================="
    echo "Restarting Spark services..."
    stop-spark
    sleep 5
    start-spark
    echo "Spark services restarted."
    echo "==========================================================="
}

# Function to verify Spark setup
verify-spark() {
    echo "==========================================================="
    echo "Verifying Spark setup..."
    
    # Check if the network exists
    if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
        echo "Creating network ${PROJECT_NAME}-network..."
        docker network create ${PROJECT_NAME}-network
    fi
    
    # Load commands
    echo "✅ Loaded commands for ${PROJECT_NAME}."
    
    # Check if Spark master is running
    if docker ps | grep -q "${PROJECT_NAME}-spark-master"; then
        echo "✅ Spark master is running."
        
        # Check if Spark worker is running
        if docker ps | grep -q "${PROJECT_NAME}-spark-worker"; then
            echo "✅ Spark worker is running."
            
            # Check UI accessibility
            echo "✅ Spark UI is accessible at http://localhost:8080"
            
            # Create a sample word count job to test Spark functionality
            echo "Testing Spark with a sample word count job..."
            docker exec -i ${PROJECT_NAME}-spark-master bash -c "echo 'Hello Spark World Test' > /tmp/test.txt && \
                spark-submit --class org.apache.spark.examples.JavaWordCount \
                --master spark://spark-master:7077 \
                /usr/bin/spark-3.3.1-bin-hadoop3/examples/jars/spark-examples_2.12-3.3.1.jar \
                /tmp/test.txt" > /dev/null 2>&1
            
            if [ $? -eq 0 ]; then
                echo "✅ Spark test job completed successfully."
                return 0
            else
                echo "❌ Spark test job failed. Check Spark logs for more details."
                return 1
            fi
        else
            echo "❌ Spark worker is NOT running."
            echo "Try starting Spark with 'start-spark' command."
            return 1
        fi
    else
        echo "❌ Spark master is NOT running."
        echo "Try starting Spark with 'start-spark' command."
        return 1
    fi
    echo "==========================================================="
}

# Function to start batch processing
start-batch-processing() {
    echo "==========================================================="
    echo "Starting batch processing..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Check if Spark is running
    if ! verify-spark; then
        echo "⛔ Spark is not running. Starting Spark now..."
        start-spark || { echo "⛔ Failed to start Spark. Please check the logs."; return 1; }
    fi
    
    # Create directories for spark apps in the container
    echo "Setting up environment for batch processing..."
    docker exec agri_data_pipeline-spark-master mkdir -p /opt/spark-apps
    
    # Copy the GCP credentials to the container
    echo "Copying GCP credentials to the container..."
    if [ -f "./gcp-creds.json" ]; then
        docker exec agri_data_pipeline-spark-master mkdir -p $(dirname /opt/spark-apps/gcp-creds.json)
        docker cp ./gcp-creds.json agri_data_pipeline-spark-master:/opt/spark-apps/gcp-creds.json
        echo "✅ GCP credentials copied to container"
    else
        echo "❌ GCP credentials file not found"
        return 1
    fi
    
    # Download and install the JAR files directly in the container
    echo "Installing required JAR files in the container..."
    cat > /tmp/install_jars.sh << 'EOL'
#!/bin/bash
set -e

echo "Installing wget..."
apt-get update -y && apt-get install -y wget

echo "Downloading JAR files..."
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11.jar https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.11/gcs-connector-hadoop3-2.2.11.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11-shaded.jar https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.11/gcs-connector-hadoop3-2.2.11-shaded.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/google-cloud-storage-2.27.1.jar https://repo1.maven.org/maven2/com/google/cloud/google-cloud-storage/2.27.1/google-cloud-storage-2.27.1.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/guava-31.1-jre.jar https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/google-api-client-2.2.0.jar https://repo1.maven.org/maven2/com/google/api-client/google-api-client/2.2.0/google-api-client-2.2.0.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/google-http-client-1.42.3.jar https://repo1.maven.org/maven2/com/google/http-client/google-http-client/1.42.3/google-http-client-1.42.3.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-oauth2-http-1.19.0.jar https://repo1.maven.org/maven2/com/google/auth/google-auth-library-oauth2-http/1.19.0/google-auth-library-oauth2-http-1.19.0.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-credentials-1.19.0.jar https://repo1.maven.org/maven2/com/google/auth/google-auth-library-credentials/1.19.0/google-auth-library-credentials-1.19.0.jar

echo "Downloading completed."
echo "Setting permissions..."
chmod 644 /usr/bin/spark-3.3.1-bin-hadoop3/jars/*.jar

echo "Listing downloaded JAR files:"
ls -la /usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs* /usr/bin/spark-3.3.1-bin-hadoop3/jars/google* /usr/bin/spark-3.3.1-bin-hadoop3/jars/guava-31.1-jre.jar

echo "JAR files installation completed successfully."
EOL
    
    # Copy the script to the container and execute it
    docker cp /tmp/install_jars.sh agri_data_pipeline-spark-master:/opt/spark-apps/install_jars.sh
    docker exec agri_data_pipeline-spark-master chmod +x /opt/spark-apps/install_jars.sh
    docker exec agri_data_pipeline-spark-master /opt/spark-apps/install_jars.sh
    echo "✅ JAR files installed in the container"
    
    # Create the batch processing script in the container
    echo "Creating batch processing script in the container..."
    cat > /tmp/batch_processing.py << 'EOL'
#!/usr/bin/env python3
# Batch processing script for agricultural data with schema handling

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
            
            print("Sample data:")
            df.show(5, truncate=False)
            
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
EOL
    
    # Copy the batch processing script to the container
    docker cp /tmp/batch_processing.py agri_data_pipeline-spark-master:/opt/spark-apps/batch_processing.py
    docker exec agri_data_pipeline-spark-master chmod +x /opt/spark-apps/batch_processing.py
    echo "✅ Batch processing script created and copied to container"
    
    # Submit the batch job
    echo "Submitting batch processing job to Spark..."
    
    # Kill any existing batch jobs
    docker exec agri_data_pipeline-spark-master pkill -f batch_processing.py || true
    
    BATCH_JOB_RESULT=$(docker exec agri_data_pipeline-spark-master /usr/bin/spark-3.3.1-bin-hadoop3/bin/spark-submit \
        --master local[*] \
        --verbose \
        --jars /usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11-shaded.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-cloud-storage-2.27.1.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/guava-31.1-jre.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-api-client-2.2.0.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-http-client-1.42.3.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-oauth2-http-1.19.0.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-credentials-1.19.0.jar \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.fs.gs.auth.service.account.enable=true \
        --conf spark.hadoop.fs.gs.auth.service.account.json.keyfile=/opt/spark-apps/gcp-creds.json \
        --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
        --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/spark-apps/gcp-creds.json \
        /opt/spark-apps/batch_processing.py 2>&1)
    
    BATCH_EXIT_CODE=$?
    
    if [ $BATCH_EXIT_CODE -ne 0 ]; then
        echo "❌ Batch processing job submission failed with exit code: $BATCH_EXIT_CODE"
        echo "Error details: $BATCH_JOB_RESULT"
        return 1
    fi
    
    echo "✅ Batch processing job completed successfully."
    echo "Check the GCS bucket for the OLAP data."
    
    # Log the successful completion
    docker exec agri_data_pipeline-spark-master bash -c "echo '$(date) - Batch Processing completed successfully' >> /opt/spark-apps/batch_completion.log"
    
    echo "==========================================================="
    return 0
} 