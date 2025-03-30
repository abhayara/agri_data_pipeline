#!/bin/bash
# Streaming and Kafka-related functions for the Agricultural Data Pipeline

# Function to start Kafka
start-kafka() {
    echo "==========================================================="
    echo "Starting Kafka services..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Start Kafka containers
    docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env up -d
    
    # Wait for containers to start
    echo "Waiting for Kafka to be ready..."
    sleep 30
    
    # Verify Kafka is running
    if docker ps | grep -q "${PROJECT_NAME}-broker"; then
        echo "✅ Kafka broker is running."
        
        # Create the topic if it doesn't exist
        echo "Checking if topic exists..."
        verify-kafka
    else
        echo "❌ Kafka broker failed to start. Check logs with 'docker logs ${PROJECT_NAME}-broker'"
        return 1
    fi
    
    echo "Kafka services started successfully."
    echo "==========================================================="
    return 0
}

# Function to stop Kafka
stop-kafka() {
    echo "==========================================================="
    echo "Stopping Kafka services..."
    docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env down
    echo "Kafka services stopped."
    echo "==========================================================="
}

# Function to restart Kafka
restart-kafka() {
    echo "==========================================================="
    echo "Restarting Kafka services..."
    stop-kafka
    sleep 5
    start-kafka
    echo "Kafka services restarted."
    echo "==========================================================="
}

# Function to verify Kafka setup
verify-kafka() {
    echo "==========================================================="
    echo "Verifying Kafka setup..."
    
    # Check if the network exists
    if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
        echo "Creating network ${PROJECT_NAME}-network..."
        docker network create ${PROJECT_NAME}-network
    fi
    
    # Check if Kafka broker is running
    if docker ps | grep -q "${PROJECT_NAME}-broker"; then
        echo "✅ Kafka broker is running."
        
        # Create topic if it doesn't exist
        echo "Checking/creating topic 'agri_data'..."
        docker exec -it ${PROJECT_NAME}-broker kafka-topics --create --if-not-exists --topic agri_data --bootstrap-server broker:9092 --replication-factor 1 --partitions 1
        
        if [ $? -eq 0 ]; then
            echo "✅ Topic 'agri_data' is available."
        else
            echo "❌ Failed to create topic. Please check Kafka logs."
            return 1
        fi
        
        # List topics to confirm
        echo "Available topics:"
        docker exec -it ${PROJECT_NAME}-broker kafka-topics --list --bootstrap-server broker:9092
        
        return 0
    else
        echo "❌ Kafka broker is NOT running."
        echo "Try starting Kafka with 'start-kafka' command."
        return 1
    fi
    
    echo "==========================================================="
}

# Function to check and fix Kafka configuration issues
check_fix_kafka_config() {
    echo "==========================================================="
    echo "Checking and fixing Kafka configuration..."
    
    # Check if the Docker Compose file exists
    KAFKA_COMPOSE_FILE="./docker/kafka/docker-compose.yml"
    if [ ! -f "$KAFKA_COMPOSE_FILE" ]; then
        echo "❌ Kafka Docker Compose file not found at $KAFKA_COMPOSE_FILE."
        return 1
    fi
    
    # Fix KAFKA_ADVERTISED_LISTENERS if needed
    if grep -q "KAFKA_ADVERTISED_LISTENERS.*localhost" "$KAFKA_COMPOSE_FILE"; then
        echo "Fixing Kafka advertised listeners configuration..."
        sed -i 's/KAFKA_ADVERTISED_LISTENERS=.*/KAFKA_ADVERTISED_LISTENERS: PLAINTEXT:\/\/broker:29092,PLAINTEXT_HOST:\/\/0.0.0.0:9092/g' "$KAFKA_COMPOSE_FILE"
        echo "✅ Fixed Kafka advertised listeners configuration."
        
        # Record this change in git
        git add "$KAFKA_COMPOSE_FILE"
        git commit -m "Fix Kafka advertised listeners configuration"
    else
        echo "✅ Kafka advertised listeners configuration is correct."
    fi
    
    # Ensure streaming files are properly configured
    STREAMING_PRODUCER="./streaming_pipeline/data_producer.py"
    STREAMING_CONSUMER="./streaming_pipeline/data_consumer.py"
    
    # Check if the GCP credentials file is copied to the streaming pipeline directory
    if [ -f "gcp-creds.json" ] && [ ! -f "./streaming_pipeline/gcp-creds.json" ]; then
        echo "Copying GCP credentials file to streaming pipeline directory..."
        cp gcp-creds.json ./streaming_pipeline/
        echo "✅ Copied GCP credentials file to streaming pipeline directory."
    fi
    
    echo "Kafka configuration is now properly set up."
    echo "==========================================================="
    return 0
}

# Function to ensure broker hostname resolution
ensure_broker_hostname_resolution() {
    echo "==========================================================="
    echo "Ensuring broker hostname resolution..."
    
    # Get the broker container IP
    BROKER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${PROJECT_NAME}-broker)
    
    if [ -z "$BROKER_IP" ]; then
        echo "❌ Could not determine broker IP address. Is the broker container running?"
        return 1
    fi
    
    echo "Broker IP: $BROKER_IP"
    
    # Create a script in the streaming_pipeline to ensure proper hostname resolution
    RESOLVE_SCRIPT="./streaming_pipeline/resolve_broker.py"
    cat > "$RESOLVE_SCRIPT" << EOF
import socket
import time

# Attempt to resolve broker hostname
def resolve_broker():
    try:
        broker_ip = socket.gethostbyname('broker')
        print(f"✅ Resolved broker to {broker_ip}")
        return True
    except socket.gaierror:
        print("❌ Could not resolve broker hostname")
        return False

# Main function
if __name__ == "__main__":
    print("Attempting to resolve broker hostname...")
    if not resolve_broker():
        print("Adding broker to /etc/hosts as a fallback...")
        with open('/etc/hosts', 'a') as hosts_file:
            hosts_file.write('$BROKER_IP broker\n')
        time.sleep(1)
        resolve_broker()
EOF
    
    echo "✅ Created broker resolution script at $RESOLVE_SCRIPT"
    
    # Record this change in git
    git add "$RESOLVE_SCRIPT"
    git commit -m "Add script to ensure broker hostname resolution"
    
    echo "Broker hostname resolution setup completed."
    echo "==========================================================="
    return 0
}

# Function to start the streaming pipeline
start-streaming-pipeline() {
    echo "==========================================================="
    echo "Starting streaming data pipeline..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Verify Kafka is running and configured
    verify-kafka || { echo "⛔ Kafka is not properly configured. Please start Kafka first."; return 1; }
    
    # Ensure broker hostname resolution
    ensure_broker_hostname_resolution
    
    # Start the streaming pipeline
    echo "Starting producer and consumer containers..."
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d --build
    
    echo "Waiting for streaming pipeline to start..."
    sleep 20
    
    # Check if producer and consumer are running
    PRODUCER_STATUS=$(docker ps -a --filter "name=agri_data_producer" --format "{{.Status}}")
    if [[ $PRODUCER_STATUS == *"Exited (0)"* ]]; then
        echo "✅ Producer completed successfully."
    elif [[ -n "$PRODUCER_STATUS" ]]; then
        echo "✅ Producer is running: $PRODUCER_STATUS"
    else
        echo "❌ Producer is not running."
        echo "Check logs with: docker logs agri_data_producer"
        return 1
    fi
    
    CONSUMER_STATUS=$(docker ps --filter "name=agri_data_consumer" --format "{{.Status}}")
    if [[ -n "$CONSUMER_STATUS" ]]; then
        echo "✅ Consumer is running: $CONSUMER_STATUS"
    else
        echo "❌ Consumer is not running."
        echo "Check logs with: docker logs agri_data_consumer"
        return 1
    fi
    
    echo "Streaming pipeline started successfully."
    echo "Consumer logs (last 10 lines):"
    docker logs agri_data_consumer | tail -n 10
    
    echo "==========================================================="
    return 0
}

# Function to stop the streaming pipeline
stop-streaming-pipeline() {
    echo "==========================================================="
    echo "Stopping streaming data pipeline..."
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env down
    echo "Streaming pipeline stopped."
    echo "==========================================================="
}

# Function to check the status of the streaming pipeline
check-streaming-status() {
    echo "==========================================================="
    echo "Checking streaming pipeline status..."
    
    # Check if producer and consumer are running
    PRODUCER_STATUS=$(docker ps -a --filter "name=agri_data_producer" --format "{{.Status}}")
    if [[ $PRODUCER_STATUS == *"Exited (0)"* ]]; then
        echo "✅ Producer completed successfully."
    elif [[ -n "$PRODUCER_STATUS" ]]; then
        echo "✅ Producer is running: $PRODUCER_STATUS"
    else
        echo "❌ Producer is not running."
    fi
    
    CONSUMER_STATUS=$(docker ps --filter "name=agri_data_consumer" --format "{{.Status}}")
    if [[ -n "$CONSUMER_STATUS" ]]; then
        echo "✅ Consumer is running: $CONSUMER_STATUS"
        echo "Consumer logs (last 10 lines):"
        docker logs agri_data_consumer | tail -n 10
    else
        echo "❌ Consumer is not running."
    fi
    
    # Check for data in GCS
    if command -v gsutil &> /dev/null; then
        echo "Checking for data in GCS bucket..."
        gsutil ls -r gs://${GCS_BUCKET_NAME}/raw/ 2>/dev/null || echo "❌ No data found in GCS bucket or bucket doesn't exist."
    else
        echo "⚠️ gsutil not available. Cannot check GCS bucket."
    fi
    
    echo "==========================================================="
    return 0
}

# Function to show all available topics in Kafka
list-kafka-topics() {
    echo "==========================================================="
    echo "Listing all Kafka topics..."
    
    if docker ps | grep -q "${PROJECT_NAME}-broker"; then
        docker exec -it ${PROJECT_NAME}-broker kafka-topics --list --bootstrap-server broker:9092
    else
        echo "❌ Kafka broker is not running."
        return 1
    fi
    
    echo "==========================================================="
    return 0
}

# Function to restart the Kafka broker with proper configuration
restart-kafka-with-config() {
    echo "==========================================================="
    echo "Restarting Kafka with proper configuration..."
    
    # Stop Kafka
    stop-kafka
    
    # Check and fix Kafka configuration
    check_fix_kafka_config
    
    # Start Kafka again
    start-kafka
    
    # Ensure hostname resolution
    ensure_broker_hostname_resolution
    
    echo "Kafka has been restarted with proper configuration."
    echo "==========================================================="
    return 0
}

# Function to check environment
check-environment() {
    echo "Checking environment requirements..."
    
    # Check for .env file
    if [ ! -f ./.env ]; then
        echo "❌ .env file is missing."
        echo "Please create one from .env.example."
        return 1
    fi
    
    # Check for GCP credentials
    if [ ! -f ./gcp-creds.json ]; then
        echo "❌ GCP credentials file (gcp-creds.json) is missing."
        return 1
    fi
    
    # Check for Docker
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker is not installed."
        return 1
    fi
    
    # Check for Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Docker Compose is not installed."
        return 1
    fi
    
    # Check for required directories
    for dir in docker streaming_pipeline batch_pipeline business_transformations terraform; do
        if [ ! -d "./$dir" ]; then
            echo "❌ Required directory '$dir' is missing."
            return 1
        fi
    done
    
    # Check Docker service
    if ! docker info &> /dev/null; then
        echo "❌ Docker service is not running."
        return 1
    fi
    
    # Copy GCP credentials to key locations if needed
    if [ -f "./gcp-creds.json" ]; then
        if [ ! -f "./streaming_pipeline/gcp-creds.json" ]; then
            echo "Copying GCP credentials to streaming_pipeline directory..."
            cp ./gcp-creds.json ./streaming_pipeline/
        fi
        
        if [ ! -f "./batch_pipeline/export_to_gcs/gcp-creds.json" ]; then
            echo "Copying GCP credentials to batch_pipeline directory..."
            mkdir -p ./batch_pipeline/export_to_gcs/
            cp ./gcp-creds.json ./batch_pipeline/export_to_gcs/
        fi
    fi
    
    echo "✅ Environment check passed."
    return 0
}

# Function to create a Git checkpoint with a custom message
git_checkpoint() {
    if [ -z "$1" ]; then
        echo "❌ No commit message provided."
        echo "Usage: git_checkpoint \"Your commit message here\""
        return 1
    fi
    
    echo "Creating Git checkpoint: $1"
    git add .
    git commit -m "$1"
    echo "✅ Git checkpoint created."
    return 0
}

# Function to display status of all components
status() {
    echo "==========================================================="
    echo "Agricultural Data Pipeline - Status"
    echo "==========================================================="
    
    # Check Kafka
    echo "Kafka:"
    if docker ps | grep -q "${PROJECT_NAME}-broker"; then
        echo "  ✅ Kafka broker is running"
    else
        echo "  ❌ Kafka broker is NOT running"
    fi
    
    # Check Spark
    echo "Spark:"
    if docker ps | grep -q "${PROJECT_NAME}-spark-master"; then
        echo "  ✅ Spark master is running"
    else
        echo "  ❌ Spark master is NOT running"
    fi
    
    if docker ps | grep -q "${PROJECT_NAME}-spark-worker"; then
        echo "  ✅ Spark worker is running"
    else
        echo "  ❌ Spark worker is NOT running"
    fi
    
    # Check streaming pipeline
    echo "Streaming Pipeline:"
    PRODUCER_STATUS=$(docker ps -a --filter "name=agri_data_producer" --format "{{.Status}}")
    if [[ $PRODUCER_STATUS == *"Exited (0)"* ]]; then
        echo "  ✅ Producer completed successfully"
    elif [[ -n "$PRODUCER_STATUS" ]]; then
        echo "  ✅ Producer is running: $PRODUCER_STATUS"
    else
        echo "  ❌ Producer is NOT running"
    fi
    
    CONSUMER_STATUS=$(docker ps --filter "name=agri_data_consumer" --format "{{.Status}}")
    if [[ -n "$CONSUMER_STATUS" ]]; then
        echo "  ✅ Consumer is running: $CONSUMER_STATUS"
    else
        echo "  ❌ Consumer is NOT running"
    fi
    
    # Check GCS and BigQuery
    echo "Cloud Resources:"
    if command -v gsutil &> /dev/null; then
        if gsutil ls gs://${GCS_BUCKET_NAME}/ &>/dev/null; then
            echo "  ✅ GCS bucket '${GCS_BUCKET_NAME}' exists"
            
            # Check for raw data
            if gsutil ls -r gs://${GCS_BUCKET_NAME}/raw/ &>/dev/null; then
                echo "  ✅ Raw data exists in GCS bucket"
            else
                echo "  ❌ No raw data found in GCS bucket"
            fi
            
            # Check for OLAP data
            if gsutil ls -r gs://${GCS_BUCKET_NAME}/olap/ &>/dev/null; then
                echo "  ✅ OLAP data exists in GCS bucket"
            else
                echo "  ❌ No OLAP data found in GCS bucket"
            fi
        else
            echo "  ❌ GCS bucket '${GCS_BUCKET_NAME}' doesn't exist or isn't accessible"
        fi
    else
        echo "  ⚠️ gsutil not available. Cannot check GCS bucket."
    fi
    
    if command -v bq &> /dev/null; then
        if bq ls ${BQ_DATASET_NAME} &>/dev/null; then
            echo "  ✅ BigQuery dataset '${BQ_DATASET_NAME}' exists"
        else
            echo "  ❌ BigQuery dataset '${BQ_DATASET_NAME}' doesn't exist or isn't accessible"
        fi
    else
        echo "  ⚠️ bq not available. Cannot check BigQuery dataset."
    fi
    
    # All services
    echo "All running services:"
    docker ps
    
    echo "==========================================================="
    return 0
}

# Function to rebuild and restart all services
rebuild-and-restart-all() {
    echo "==========================================================="
    echo "Rebuilding and restarting all services..."
    echo "==========================================================="
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Stop all services
    echo "Stopping all services..."
    stop-streaming-pipeline
    stop-spark
    stop-kafka
    
    # Clean up Docker resources
    echo "Cleaning up Docker resources..."
    docker system prune -f
    
    # Fix configurations
    echo "Fixing configurations..."
    check_fix_kafka_config
    check_fix_gcs_config
    
    # Start services in the correct order
    echo "Starting services in the correct order..."
    
    # 1. Start Kafka
    start-kafka || { echo "⛔ Failed to start Kafka."; return 1; }
    
    # 2. Start Spark
    start-spark || { echo "⛔ Failed to start Spark."; return 1; }
    
    # 3. Start streaming pipeline
    start-streaming-pipeline || { echo "⛔ Failed to start streaming pipeline."; return 1; }
    
    # 4. Run batch pipeline
    echo "Running batch pipeline..."
    start-batch-pipeline || { echo "⚠️ Batch pipeline had errors. Check logs."; }
    
    # 5. Run DBT transformations
    echo "Running DBT transformations..."
    run-dbt || { echo "⚠️ DBT transformations had errors. Check logs."; }
    
    echo "Rebuild and restart completed."
    echo "==========================================================="
    
    # Show current status
    status
    
    return 0
} 