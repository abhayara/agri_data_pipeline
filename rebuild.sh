#!/bin/bash

# Automated rebuild script for Agricultural Data Pipeline
# This script will:
# 1. Check the environment
# 2. Stop all running services
# 3. Update configurations for better network compatibility
# 4. Rebuild and restart all services
# 5. Create Git checkpoints along the way

# Make sure we're in the project root directory
cd "$(dirname "$0")"

echo "==========================================================="
echo "Agricultural Data Pipeline - Automated Rebuild"
echo "Version: 1.0.0"
echo "Date: $(date)"
echo "==========================================================="

# Source the commands
echo "Loading commands..."
source ./commands.sh

# Step 1: Check environment
echo "Step 1: Checking environment..."
check-environment || { 
    echo "⛔ Environment check failed. Please fix the issues before continuing."
    exit 1
}

# Step 2: Stop all running services
echo "Step 2: Stopping all services..."
stop-streaming-pipeline
stop-spark
stop-kafka
stop-postgres
stop-metabase

# Wait for containers to stop
echo "Waiting for all containers to stop..."
sleep 10

# Step 3: Update configurations
echo "Step 3: Updating configurations..."
check_fix_kafka_config
check_fix_gcs_config

# Check if the .env file has the GCP_LOCATION set for asia-south1
if ! grep -q "GCP_LOCATION=asia-south1" ./.env; then
    echo "Updating GCP location to asia-south1 in .env file..."
    echo "GCP_LOCATION=asia-south1" >> ./.env
    echo "✅ Added GCP_LOCATION to .env file."
fi

# Create a git checkpoint
git_checkpoint "Configuration updated for better compatibility"

# Step 4: Rebuild and restart all services
echo "Step 4: Rebuilding and restarting all services..."

# Start Postgres
echo "Starting Postgres..."
start-postgres

# Start Kafka with the updated configuration
echo "Starting Kafka..."
start-kafka

echo "Waiting for Kafka to be ready..."
sleep 15

echo "Verifying and configuring Kafka..."
verify-kafka
ensure_broker_hostname_resolution

# Start Spark
echo "Starting Spark..."
start-spark

# Verify Spark (this will also check GCS config since we updated the verify-spark function)
echo "Verifying Spark setup..."
verify-spark

# Start Metabase
echo "Starting Metabase..."
start-metabase

# Create another git checkpoint
git_checkpoint "Infrastructure services restarted successfully"

# Start the streaming pipeline in detached mode
echo "Starting streaming pipeline with producer and consumer..."
docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d --build

echo "All services have been rebuilt and restarted."

# Show status
echo "Current status of the pipeline:"
status

echo "==========================================================="
echo "Rebuild completed successfully."
echo "To see the logs from the producer/consumer, run:"
echo "  docker logs -f agri_data_producer"
echo "  docker logs -f agri_data_consumer"
echo "===========================================================" 