#!/bin/bash
# Batch pipeline-related functions for the Agricultural Data Pipeline

# Function to fix and restart the batch pipeline
fix-batch-pipeline() {
    echo "==========================================================="
    echo "Fixing and restarting the batch pipeline..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Fix GCS configuration
    check_fix_gcs_config || { echo "⛔ GCS configuration fix failed."; return 1; }
    
    # Restart Spark to apply changes
    echo "Restarting Spark services to apply changes..."
    stop-spark
    sleep 5
    start-spark || { echo "⛔ Failed to start Spark services."; return 1; }
    
    # Wait for Spark to be fully operational
    echo "Waiting for Spark to be fully operational..."
    sleep 15
    
    # Run the batch pipeline
    echo "Running the batch pipeline with fixed configuration..."
    start-batch-pipeline
    
    echo "Batch pipeline fix and restart process completed."
    echo "==========================================================="
    return 0
}

# Function to start the batch pipeline
start-batch-pipeline() {
    echo "==========================================================="
    echo "STEP 1: Processing data with batch pipeline..."
    echo "==========================================================="
    
    # Check if network exists
    if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
        docker network create ${PROJECT_NAME}-network
        echo "Created network ${PROJECT_NAME}-network."
    else
        echo "Network ${PROJECT_NAME}-network already exists."
    fi
    
    # Load commands
    echo "✅ Loaded commands for ${PROJECT_NAME}."
    
    # Check environment
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Fix GCS connector configuration
    check_fix_gcs_config
    
    # Run OLAP transformations to prepare data
    echo "Running OLAP transformations..."
    
    # Check environment again
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Check if there is data in the /tmp/agri_data directory
    if [ ! -d "/tmp/agri_data" ] || [ -z "$(ls -A /tmp/agri_data 2>/dev/null)" ]; then
        echo "⚠️ Warning: No data found in /tmp/agri_data."
        echo "The streaming pipeline may not have processed any data yet."
        echo "Proceeding with sample data generation..."
    fi
    
    # Run PySpark transformation
    echo "Starting PySpark transformation..."
    cd ./batch_pipeline/export_to_gcs || { echo "❌ Could not find batch_pipeline directory."; return 1; }
    python3 pipeline.py > /tmp/batch_pipeline.log 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✅ Batch pipeline completed successfully."
    else
        echo "❌ Batch pipeline encountered errors."
        echo "Error details:"
        grep -A 10 "Error" /tmp/batch_pipeline.log | head -20
        echo "See /tmp/batch_pipeline.log for full details."
    fi
    
    cd ../..
    
    echo "==========================================================="
    echo "STEP 2: Loading transformed data to BigQuery..."
    echo "==========================================================="
    
    # Start GCS to BigQuery export pipeline
    echo "Starting GCS to BigQuery export pipeline..."
    cd ./batch_pipeline/export_to_big_query || { echo "❌ Could not find export_to_big_query directory."; return 1; }
    
    # Check if Airflow is running
    if ! docker ps | grep -q "airflow-airflow-webserver-1"; then
        echo "❌ Airflow is not running. Cannot trigger the DAG."
        echo "  Please start Airflow with 'start-airflow' and try again."
        cd ../..
        return 1
    fi
    
    # Trigger DAG in Airflow
    docker exec airflow-airflow-webserver-1 airflow dags trigger gcs_to_bigquery_dag
    
    if [ $? -eq 0 ]; then
        echo "✅ GCS to BigQuery export pipeline triggered successfully."
        echo "  Check Airflow UI at http://localhost:8080 for progress."
    else
        echo "❌ GCS to BigQuery export pipeline failed with exit code $?."
        echo "  Please check the logs for more details."
    fi
    
    cd ../..
    
    echo "==========================================================="
    echo "Batch pipeline process completed."
    echo "==========================================================="
}

# Function to verify the batch pipeline
verify-batch-pipeline() {
    echo "==========================================================="
    echo "Verifying batch pipeline setup..."
    
    # Check environment variables
    if [ -z "$GCS_BUCKET_NAME" ]; then
        echo "❌ GCS_BUCKET_NAME environment variable is not set."
        source .env 2>/dev/null || echo "⚠️ Could not source .env file. Using default values."
    fi
    
    # Check if batch_pipeline directory exists
    if [ ! -d "batch_pipeline" ]; then
        echo "❌ batch_pipeline directory not found."
        return 1
    else
        echo "✅ batch_pipeline directory found."
    fi
    
    # Check for GCS connector configuration
    if [ -f "batch_pipeline/export_to_gcs/pipeline.py" ]; then
        if grep -q "GoogleHadoopFileSystem" batch_pipeline/export_to_gcs/pipeline.py; then
            echo "✅ GCS connector configuration is present in pipeline.py."
        else
            echo "❌ GCS connector configuration is missing in pipeline.py."
            echo "   Run check_fix_gcs_config to fix this issue."
        fi
    else
        echo "❌ pipeline.py not found in batch_pipeline/export_to_gcs."
        return 1
    fi
    
    # Check GCP credentials
    if [ -f "gcp-creds.json" ]; then
        echo "✅ GCP credentials file found."
        if [ ! -f "batch_pipeline/export_to_gcs/gcp-creds.json" ]; then
            echo "Copying gcp-creds.json to batch_pipeline/export_to_gcs..."
            cp gcp-creds.json batch_pipeline/export_to_gcs/
        fi
    else
        echo "❌ GCP credentials file (gcp-creds.json) not found."
        return 1
    fi
    
    # Check Spark setup
    if docker ps | grep -q "${PROJECT_NAME}-spark-master"; then
        echo "✅ Spark master is running."
    else
        echo "❌ Spark master is not running."
        echo "   Start Spark with 'start-spark' command."
    fi
    
    # Summarize verification
    echo
    echo "Batch Pipeline Configuration Summary:"
    echo "- GCS Bucket: ${GCS_BUCKET_NAME:-"not set"}"
    echo "- Raw data path: ${GCS_RAW_DATA_PATH:-"not set"}"
    echo
    echo "Check the following components to ensure proper operation:"
    echo "1. Verify Spark is running with 'verify-spark'"
    echo "2. Verify data is being consumed from Kafka with 'verify-consumer'"
    echo "3. Verify Airflow is running for BigQuery export with 'verify-airflow'"
    echo
    
    echo "Batch pipeline verification completed."
    echo "==========================================================="
    return 0
}

# Function to check and fix GCS connector configuration
check_fix_gcs_config() {
    echo "==========================================================="
    echo "Checking and fixing GCS connector configuration..."
    
    GCS_PIPELINE_FILE="./batch_pipeline/export_to_gcs/pipeline.py"
    
    if [ -f "$GCS_PIPELINE_FILE" ]; then
        # Check if GCS connector is properly configured
        if ! grep -q "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11" "$GCS_PIPELINE_FILE"; then
            echo "Updating PySpark GCS connector configuration..."
            sed -i 's/spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1/spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11,com.google.guava:guava:31.1-jre/g' "$GCS_PIPELINE_FILE"
            sed -i '/GOOGLE_APPLICATION_CREDENTIALS)/ a\        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \\n        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")' "$GCS_PIPELINE_FILE"
            echo "✅ Updated GCS connector configuration in pipeline.py file."
            
            # Record this change in git
            git add "$GCS_PIPELINE_FILE"
            git commit -m "Update GCS connector configuration for Spark"
        else
            echo "✅ GCS connector configuration is already present in pipeline.py file."
        fi
        
        # Ensure GCP credentials file exists in both locations
        if [ -f "./gcp-creds.json" ] && [ ! -f "./batch_pipeline/export_to_gcs/gcp-creds.json" ]; then
            echo "Copying GCP credentials file to batch_pipeline directory..."
            cp ./gcp-creds.json ./batch_pipeline/export_to_gcs/
            echo "✅ Copied GCP credentials file to batch_pipeline directory."
        fi
        
        # Check for type mismatches in sample data generation
        if ! grep -q "irrigation_amount = 0.0 if" "$GCS_PIPELINE_FILE"; then
            echo "Fixing type mismatch issues in sample data generation..."
            sed -i 's/irrigation_amount = 0 if/irrigation_amount = 0.0 if/g' "$GCS_PIPELINE_FILE"
            sed -i 's/fertilizer_amount = 0 if/fertilizer_amount = 0.0 if/g' "$GCS_PIPELINE_FILE"
            sed -i 's/pesticide_amount = 0 if/pesticide_amount = 0.0 if/g' "$GCS_PIPELINE_FILE"
            echo "✅ Fixed type mismatch issues in sample data generation."
            
            # Record this change in git
            git add "$GCS_PIPELINE_FILE"
            git commit -m "Fix type mismatch issues in sample data generation"
        else
            echo "✅ Type handling is already correctly configured."
        fi
        
        # Check for extraClassPath configuration - remove hardcoded paths
        if grep -q "/.ivy2/jars/" "$GCS_PIPELINE_FILE"; then
            echo "Fixing extraClassPath configuration to use relative paths..."
            # Remove hardcoded paths for extraClassPath
            sed -i 's/.config("spark.driver.extraClassPath", "\/home\/abhay\/.ivy2\/jars\/\*")/.config("spark.driver.extraClassPath", "\/usr\/local\/spark\/jars\/\*")/g' "$GCS_PIPELINE_FILE"
            sed -i 's/.config("spark.executor.extraClassPath", "\/home\/abhay\/.ivy2\/jars\/\*")/.config("spark.executor.extraClassPath", "\/usr\/local\/spark\/jars\/\*")/g' "$GCS_PIPELINE_FILE"
            echo "✅ Fixed extraClassPath configuration."
            
            # Record this change in git
            git add "$GCS_PIPELINE_FILE"
            git commit -m "Fix extraClassPath configuration in pipeline.py"
        fi
        
        # Download the necessary JARs if not already present
        echo "Checking for necessary JAR dependencies..."
        LOCAL_JAR_DIR="./batch_pipeline/jars"
        mkdir -p $LOCAL_JAR_DIR
        
        # Define the necessary JAR files
        JARS=(
            "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.11/gcs-connector-hadoop3-2.2.11.jar"
            "https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar"
        )
        
        # Download each JAR file if it's not already present
        for jar_url in "${JARS[@]}"; do
            jar_name=$(basename $jar_url)
            if [ ! -f "$LOCAL_JAR_DIR/$jar_name" ]; then
                echo "Downloading $jar_name..."
                if command -v wget &> /dev/null; then
                    wget -q -O "$LOCAL_JAR_DIR/$jar_name" $jar_url
                elif command -v curl &> /dev/null; then
                    curl -s -o "$LOCAL_JAR_DIR/$jar_name" $jar_url
                else
                    echo "❌ Neither wget nor curl is available. Cannot download JARs."
                    echo "Please install wget or curl, or manually download the needed JARs."
                fi
                
                if [ -f "$LOCAL_JAR_DIR/$jar_name" ]; then
                    echo "✅ Downloaded $jar_name successfully."
                else
                    echo "❌ Failed to download $jar_name."
                fi
            else
                echo "✅ $jar_name already exists."
            fi
        done
        
        # Add a script to install jars in the docker container
        INSTALL_SCRIPT="./batch_pipeline/install_jars.sh"
        if [ ! -f "$INSTALL_SCRIPT" ]; then
            echo "Creating installation script for Spark JARs..."
            cat > "$INSTALL_SCRIPT" << 'EOF'
#!/bin/bash
# Script to install GCS connector jars to the Spark container

CONTAINER_NAME="agri_data_pipeline-spark-master"
LOCAL_JAR_DIR="./batch_pipeline/jars"
CONTAINER_JAR_DIR="/usr/local/spark/jars"

if [ ! -d "$LOCAL_JAR_DIR" ]; then
    echo "JAR directory not found at $LOCAL_JAR_DIR"
    exit 1
fi

if ! docker ps | grep -q "$CONTAINER_NAME"; then
    echo "Spark container $CONTAINER_NAME is not running"
    exit 1
fi

# Copy each JAR file to the Spark container
for jar_file in "$LOCAL_JAR_DIR"/*.jar; do
    if [ -f "$jar_file" ]; then
        jar_name=$(basename "$jar_file")
        echo "Copying $jar_name to Spark container..."
        docker cp "$jar_file" "$CONTAINER_NAME:$CONTAINER_JAR_DIR/$jar_name"
        if [ $? -eq 0 ]; then
            echo "✅ Successfully copied $jar_name to Spark container"
        else
            echo "❌ Failed to copy $jar_name to Spark container"
        fi
    fi
done

echo "All JARs installed to Spark container"
EOF
            chmod +x "$INSTALL_SCRIPT"
            echo "✅ Created installation script at $INSTALL_SCRIPT"
            
            # Record this change in git
            git add "$INSTALL_SCRIPT"
            git commit -m "Add script to install GCS connector JARs to Spark container"
        fi
        
        # Run the installation script if Spark is running
        if docker ps | grep -q "agri_data_pipeline-spark-master"; then
            echo "Installing JARs to Spark container..."
            $INSTALL_SCRIPT
            echo "✅ JARs installed to Spark container"
        else
            echo "⚠️ Spark container is not running. JARs will be installed when Spark is started."
        fi
    else
        echo "❌ Batch pipeline file not found at $GCS_PIPELINE_FILE."
        return 1
    fi
    
    echo "GCS connector configuration is now properly set up."
    echo "==========================================================="
    return 0
}

# Function to run the OLAP transformation pipeline
olap-transformation-pipeline(){
    echo "==========================================================="
    echo "Starting OLAP transformation pipeline..."
    echo "==========================================================="
    
    # Check environment
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Check if Spark is running
    if ! docker ps | grep -q "${PROJECT_NAME}-spark-master"; then
        echo "❌ Spark is not running. Please start Spark first."
        return 1
    fi
    
    # Run the OLAP transformation script
    cd ./batch_pipeline/export_to_gcs || { echo "❌ Could not find batch_pipeline directory."; return 1; }
    python3 pipeline.py
    
    if [ $? -eq 0 ]; then
        echo "✅ OLAP transformations completed successfully."
    else
        echo "❌ OLAP transformations failed."
        echo "Check the logs for more details."
        cd ../..
        return 1
    fi
    
    cd ../..
    echo "==========================================================="
    return 0
}

# Function to run the GCS to BigQuery export pipeline
gcs-to-bigquery-pipeline(){
    echo "==========================================================="
    echo "Starting GCS to BigQuery export pipeline..."
    
    # Check if batch_pipeline directory exists
    if [ ! -d "batch_pipeline/export_to_big_query" ]; then
        echo "❌ batch_pipeline/export_to_big_query directory not found."
        return 1
    fi
    
    # Check if Airflow is running
    if ! docker ps | grep -q "airflow-airflow-webserver-1"; then
        echo "❌ Airflow is not running. Please start Airflow first."
        echo "  Use 'start-airflow' command."
        return 1
    fi
    
    # Verify DAG exists in Airflow
    if ! docker exec airflow-airflow-webserver-1 airflow dags list 2>/dev/null | grep -q "gcs_to_bigquery_dag"; then
        echo "❌ gcs_to_bigquery_dag not found in Airflow."
        echo "  Check your Airflow setup."
        return 1
    fi
    
    # Trigger the DAG
    echo "Triggering GCS to BigQuery export DAG..."
    docker exec airflow-airflow-webserver-1 airflow dags trigger gcs_to_bigquery_dag
    
    if [ $? -eq 0 ]; then
        echo "✅ GCS to BigQuery export DAG triggered successfully."
        echo "  Check Airflow UI at http://localhost:8080 for progress."
    else
        echo "❌ Failed to trigger DAG. HTTP response: $?"
        echo "  Please check the Airflow logs for more details."
    fi
    
    echo "==========================================================="
} 