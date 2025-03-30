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

# Create the jars directory if it doesn't exist
docker exec $CONTAINER_NAME mkdir -p $CONTAINER_JAR_DIR

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
echo "==========================================================="

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

