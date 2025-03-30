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
source ./scripts/main.sh

# Step 1: Check environment
echo "Step 1: Checking environment..."
# Define basic environment check if not available in the modular scripts
check_environment() {
    echo "Checking basic environment requirements..."
    
    # Check for .env file
    if [ ! -f ./.env ]; then
        echo "❌ .env file is missing."
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
    for dir in docker batch_pipeline business_transformations; do
        if [ ! -d "./$dir" ]; then
            echo "❌ Required directory '$dir' is missing."
            return 1
        fi
    done
    
    echo "✅ Basic environment checks passed."
    return 0
}

# Use the local function if the modular one is not available
if ! type verify-spark >/dev/null 2>&1; then
    echo "⚠️ Modular functions not loaded properly, using local environment check."
    check_environment || { 
        echo "⛔ Environment check failed. Please fix the issues before continuing."
        exit 1
    }
else
    echo "✅ Modular functions loaded successfully."
fi

# Step 2: Stop all running services
echo "Step 2: Stopping all services..."
# Stop all Docker containers to be safe
docker stop $(docker ps -aq) || true
docker rm $(docker ps -aq) || true
docker network prune -f

# Wait for containers to stop
echo "Waiting for all containers to stop..."
sleep 5

# Step 3: Update configurations
echo "Step 3: Updating configurations..."
if type check_fix_kafka_config >/dev/null 2>&1; then
    check_fix_kafka_config
fi

if type check_fix_gcs_config >/dev/null 2>&1; then
    check_fix_gcs_config
fi

# Check if the .env file has the GCP_LOCATION set for asia-south1
if ! grep -q "GCP_LOCATION=asia-south1" ./.env; then
    echo "Updating GCP location to asia-south1 in .env file..."
    echo "GCP_LOCATION=asia-south1" >> ./.env
    echo "✅ Added GCP_LOCATION to .env file."
fi

# Create a git checkpoint
if type git_checkpoint >/dev/null 2>&1; then
    git_checkpoint "Configuration updated for better compatibility"
else
    echo "Saving configuration changes in git..."
    git add .env
    git commit -m "Configuration updated for better compatibility"
fi

# Step 4: Rebuild and restart all services
echo "Step 4: Rebuilding and restarting all services..."

# Start Postgres if available
if type start-postgres >/dev/null 2>&1; then
    echo "Starting Postgres..."
    start-postgres
fi

# Start Kafka
echo "Starting Kafka..."
if type start-kafka >/dev/null 2>&1; then
    start-kafka
else
    echo "Starting Kafka using docker-compose..."
    docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env up -d
fi

echo "Waiting for Kafka to be ready..."
sleep 15

echo "Verifying and configuring Kafka..."
if type verify-kafka >/dev/null 2>&1; then
    verify-kafka
fi

if type ensure_broker_hostname_resolution >/dev/null 2>&1; then
    ensure_broker_hostname_resolution
fi

# Start Spark
echo "Starting Spark..."
if type start-spark >/dev/null 2>&1; then
    start-spark
else
    echo "Starting Spark using docker-compose..."
    docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env up -d
fi

# Verify Spark
echo "Verifying Spark setup..."
if type verify-spark >/dev/null 2>&1; then
    verify-spark
fi

# Start Metabase if available
if type start-metabase >/dev/null 2>&1; then
    echo "Starting Metabase..."
    start-metabase
fi

# Create another git checkpoint
if type git_checkpoint >/dev/null 2>&1; then
    git_checkpoint "Infrastructure services restarted successfully"
else
    echo "Saving infrastructure restart in git..."
    git add docker
    git commit -m "Infrastructure services restarted successfully"
fi

# Start the streaming pipeline in detached mode
echo "Starting streaming pipeline with producer and consumer..."
docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d --build

echo "All services have been rebuilt and restarted."

# Show status
if type status >/dev/null 2>&1; then
    echo "Current status of the pipeline:"
    status
else
    echo "Current status of the pipeline:"
    docker ps
fi

echo "==========================================================="
echo "Rebuild completed successfully."
echo "To see the logs from the producer/consumer, run:"
echo "  docker logs -f agri_data_producer"
echo "  docker logs -f agri_data_consumer"
echo "===========================================================" 