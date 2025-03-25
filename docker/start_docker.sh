#!/bin/bash

# Set project name if not set
if [ -z "$PROJECT_NAME" ]; then
    export PROJECT_NAME="agri_data_pipeline"
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Error: .env file not found. Please create one from .env.example"
    echo "cp .env.example .env"
    exit 1
fi

# Load environment variables
source .env

# Create Docker network and volume
echo "Creating Docker network and volumes..."
docker network create "${PROJECT_NAME}-network" 2>/dev/null || true
docker volume create --name=hadoop-distributed-file-system 2>/dev/null || true

# Start containers
start_containers() {
    echo "Starting $1 related containers"
    docker-compose -f "$2" --env-file .env up -d
    echo "Waiting for $1 services to initialize..."
    sleep 5
    echo
}

echo "Starting services in dependency order..."

# Start services in order of dependencies
start_containers "Postgres" "./docker/postgres/docker-compose.yml"
sleep 10 # Extra time for Postgres to initialize

start_containers "Kafka" "./docker/kafka/docker-compose.yml"
sleep 15 # Extra time for Kafka to initialize

start_containers "Airflow" "./docker/airflow/docker-compose.yml"
start_containers "Metabase" "./docker/metabase/docker-compose.yml"

echo "Starting Spark related containers"
chmod +x ./docker/spark/build.sh
./docker/spark/build.sh
echo
docker-compose -f ./docker/spark/docker-compose.yml --env-file .env up -d
echo

echo "All containers started. Checking service health..."
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo

echo "Started all containers. You can check their status with 'docker ps'."