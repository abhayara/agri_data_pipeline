#!/bin/bash

# Agriculture Data Pipeline - Unified Build Script
# This script manages the entire lifecycle of the agriculture data pipeline
# Author: Abhay
# Date: $(date)

# Set colors for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;36m'
NC='\033[0m' # No Color

# Export environment variables
export PROJECT_NAME="agri_data_pipeline"
export PROJECT_ROOT=$(pwd)

# Load environment variables if available
if [ -f ".env" ]; then
    source .env
    echo -e "${GREEN}Loaded environment variables from .env file${NC}"
else
    echo -e "${RED}ERROR: .env file not found. Please create one from .env.example${NC}"
    exit 1
fi

# Function to print section headers
print_section() {
    echo -e "\n${BLUE}===========================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===========================================================${NC}"
}

# Function to check environment
check_environment() {
    print_section "Checking Environment"
    
    # Check for required files
    if [ ! -f "./gcp-creds.json" ]; then
        echo -e "${RED}ERROR: GCP credentials file (gcp-creds.json) is missing${NC}"
        return 1
    fi
    
    # Check for required commands
    for cmd in docker docker-compose terraform pip python git; do
        if ! command -v $cmd &> /dev/null; then
            echo -e "${RED}ERROR: $cmd is not installed${NC}"
            return 1
        fi
    done
    
    # Check for required directories
    for dir in docker terraform streaming_pipeline batch_pipeline business_transformations; do
        if [ ! -d "./$dir" ]; then
            echo -e "${RED}ERROR: Required directory '$dir' is missing${NC}"
            return 1
        fi
    done
    
    # Check Docker service
    if ! docker info &> /dev/null; then
        echo -e "${RED}ERROR: Docker service is not running${NC}"
        return 1
    fi
    
    echo -e "${GREEN}Environment check passed${NC}"
    return 0
}

# Function to destroy all infrastructure
destroy_infrastructure() {
    print_section "Destroying Infrastructure"
    
    echo -e "${YELLOW}Stopping all running containers...${NC}"
    docker-compose -f ./docker/streaming/docker-compose.yml down 2>/dev/null || true
    docker-compose -f ./docker/kafka/docker-compose.yml down 2>/dev/null || true
    docker-compose -f ./docker/spark/docker-compose.yml down 2>/dev/null || true
    docker-compose -f ./docker/airflow/docker-compose.yml down 2>/dev/null || true
    docker-compose -f ./docker/metabase/docker-compose.yml down 2>/dev/null || true
    docker-compose -f ./docker/postgres/docker-compose.yml down 2>/dev/null || true
    
    echo -e "${YELLOW}Removing any remaining project containers...${NC}"
    docker ps -a | grep 'agri_data' | awk '{print $1}' | xargs docker rm -f 2>/dev/null || true
    
    echo -e "${YELLOW}Destroying Terraform resources...${NC}"
    if [ -d "./terraform" ]; then
        terraform -chdir=terraform destroy -auto-approve
        echo -e "${GREEN}Terraform resources destroyed${NC}"
    else
        echo -e "${RED}Terraform directory not found. Cannot destroy resources${NC}"
    fi
}

# Function to clean Docker resources
clean_docker_resources() {
    print_section "Cleaning Docker Resources"
    
    echo -e "${YELLOW}Pruning Docker volumes...${NC}"
    docker volume prune -f
    
    echo -e "${YELLOW}Pruning Docker networks...${NC}"
    docker network prune -f
    
    echo -e "${YELLOW}Pruning Docker images...${NC}"
    docker image prune -f
    
    echo -e "${YELLOW}Removing all project-related volumes...${NC}"
    docker volume ls | grep 'agri_data' | awk '{print $2}' | xargs docker volume rm 2>/dev/null || true
    
    echo -e "${GREEN}Docker resources cleaned${NC}"
}

# Function to initialize infrastructure
initialize_infrastructure() {
    print_section "Initializing Infrastructure"
    
    # Create Docker network if it doesn't exist
    if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
        echo -e "${YELLOW}Creating Docker network...${NC}"
        docker network create ${PROJECT_NAME}-network
        echo -e "${GREEN}Created network ${PROJECT_NAME}-network${NC}"
    else
        echo -e "${GREEN}Network ${PROJECT_NAME}-network already exists${NC}"
    fi
    
    # Initialize Terraform
    echo -e "${YELLOW}Initializing Terraform...${NC}"
    if [ -d "./terraform" ]; then
        terraform -chdir=terraform init
        echo -e "${GREEN}Terraform initialized${NC}"
        
        # Apply Terraform configuration
        echo -e "${YELLOW}Applying Terraform configuration...${NC}"
        terraform -chdir=terraform apply -auto-approve
        echo -e "${GREEN}Terraform resources created${NC}"
    else
        echo -e "${RED}Terraform directory not found. Cannot initialize infrastructure${NC}"
        return 1
    fi
    
    # Install required Python packages
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    pip install -r requirements.txt
    echo -e "${GREEN}Python dependencies installed${NC}"
    
    return 0
}

# Function to start Kafka
start_kafka() {
    print_section "Starting Kafka"
    
    echo -e "${YELLOW}Starting Kafka and Zookeeper...${NC}"
    docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env up -d
    
    echo -e "${YELLOW}Waiting for Kafka to be ready...${NC}"
    sleep 30
    
    # Verify Kafka is running
    if docker ps | grep -q "${PROJECT_NAME}-broker"; then
        echo -e "${GREEN}Kafka is running${NC}"
    else
        echo -e "${RED}Kafka failed to start${NC}"
        return 1
    fi
    
    return 0
}

# Function to start Spark
start_spark() {
    print_section "Starting Spark"
    
    echo -e "${YELLOW}Starting Spark master and worker...${NC}"
    docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env up -d
    
    echo -e "${YELLOW}Waiting for Spark to be ready...${NC}"
    sleep 20
    
    # Verify Spark is running
    if docker ps | grep -q "${PROJECT_NAME}-spark-master"; then
        echo -e "${GREEN}Spark is running${NC}"
    else
        echo -e "${RED}Spark failed to start${NC}"
        return 1
    fi
    
    return 0
}

# Function to generate data and start streaming pipeline
start_streaming_pipeline() {
    print_section "Starting Streaming Pipeline"
    
    echo -e "${YELLOW}Starting data producer and consumer...${NC}"
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d --build
    
    echo -e "${YELLOW}Waiting for streaming pipeline to start...${NC}"
    sleep 20
    
    # Verify producer and consumer are running
    PRODUCER_STATUS=$(docker ps -a --filter "name=agri_data_producer" --format "{{.Status}}")
    if [[ $PRODUCER_STATUS == *"Exited (0)"* ]]; then
        echo -e "${GREEN}Producer completed successfully${NC}"
    else
        echo -e "${YELLOW}Producer status: $PRODUCER_STATUS${NC}"
    fi
    
    CONSUMER_STATUS=$(docker ps --filter "name=agri_data_consumer" --format "{{.Status}}")
    if [[ -n "$CONSUMER_STATUS" ]]; then
        echo -e "${GREEN}Consumer is running${NC}"
    else
        echo -e "${RED}Consumer is not running${NC}"
        return 1
    fi
    
    echo -e "${YELLOW}Streaming pipeline logs:${NC}"
    docker logs agri_data_consumer | tail -n 10
    
    return 0
}

# Function to run batch pipeline
run_batch_pipeline() {
    print_section "Running Batch Pipeline"
    
    echo -e "${YELLOW}Installing Spark GCS connector JARs...${NC}"
    bash ./batch_pipeline/install_jars.sh
    
    echo -e "${YELLOW}Running Spark job to process data from GCS...${NC}"
    cd batch_pipeline/export_to_gcs
    python pipeline.py
    cd ../../
    
    echo -e "${YELLOW}Exporting processed data to BigQuery...${NC}"
    cd batch_pipeline/export_to_big_query
    python export_to_bq.py
    cd ../../
    
    echo -e "${GREEN}Batch pipeline completed${NC}"
    return 0
}

# Function to run DBT transformations
run_dbt_transformations() {
    print_section "Running DBT Transformations"
    
    cd business_transformations
    
    echo -e "${YELLOW}Verifying DBT configuration...${NC}"
    dbt debug --profiles-dir .
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}DBT configuration has issues${NC}"
        cd ..
        return 1
    fi
    
    echo -e "${YELLOW}Running DBT models...${NC}"
    dbt run --profiles-dir .
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}DBT models built successfully${NC}"
        
        echo -e "${YELLOW}Testing DBT models...${NC}"
        dbt test --profiles-dir .
        
        echo -e "${YELLOW}Generating DBT documentation...${NC}"
        dbt docs generate --profiles-dir .
    else
        echo -e "${RED}DBT model build failed${NC}"
        cd ..
        return 1
    fi
    
    cd ..
    echo -e "${GREEN}DBT transformations completed${NC}"
    return 0
}

# Function to display usage
usage() {
    echo -e "Usage: $0 [OPTION]"
    echo -e "Options:"
    echo -e "  --help            Display this help message"
    echo -e "  --rebuild         Rebuild the entire pipeline (default action)"
    echo -e "  --clean-only      Only clean resources without rebuilding"
    echo -e "  --status          Display the status of all pipeline components"
    echo -e "  --streaming-only  Only start the streaming pipeline"
    echo -e "  --batch-only      Only run the batch pipeline"
    echo -e "  --dbt-only        Only run the DBT transformations"
}

# Function to display status
show_status() {
    print_section "Pipeline Status"
    
    echo -e "${YELLOW}Docker containers:${NC}"
    docker ps
    
    echo -e "\n${YELLOW}GCS Bucket:${NC}"
    if command -v gsutil &> /dev/null; then
        gsutil ls gs://${GCS_BUCKET_NAME}/ 2>/dev/null || echo -e "${RED}Cannot access GCS bucket${NC}"
    else
        echo -e "${RED}gsutil not available${NC}"
    fi
    
    echo -e "\n${YELLOW}BigQuery Dataset:${NC}"
    if command -v bq &> /dev/null; then
        bq ls ${BQ_DATASET_NAME} 2>/dev/null || echo -e "${RED}Cannot access BigQuery dataset${NC}"
    else
        echo -e "${RED}bq not available${NC}"
    fi
}

# Main execution
main() {
    if [ "$1" == "--help" ]; then
        usage
        exit 0
    fi
    
    if [ "$1" == "--status" ]; then
        show_status
        exit 0
    fi
    
    # Check environment
    check_environment || {
        echo -e "${RED}Environment check failed. Please fix the issues before continuing${NC}"
        exit 1
    }
    
    if [ "$1" == "--clean-only" ]; then
        destroy_infrastructure
        clean_docker_resources
        exit 0
    fi
    
    if [ "$1" == "--streaming-only" ]; then
        start_kafka && start_streaming_pipeline
        exit $?
    fi
    
    if [ "$1" == "--batch-only" ]; then
        start_spark && run_batch_pipeline
        exit $?
    fi
    
    if [ "$1" == "--dbt-only" ]; then
        run_dbt_transformations
        exit $?
    fi
    
    # Default action: rebuild everything
    print_section "Full Pipeline Rebuild"
    
    # 1. Destroy all infrastructure
    destroy_infrastructure
    
    # 2. Clean Docker resources
    clean_docker_resources
    
    # 3. Initialize infrastructure
    initialize_infrastructure || exit 1
    
    # 4. Start Kafka
    start_kafka || exit 1
    
    # 5. Start Spark
    start_spark || exit 1
    
    # 6. Start streaming pipeline
    start_streaming_pipeline || exit 1
    
    # 7. Run batch pipeline
    run_batch_pipeline || exit 1
    
    # 8. Run DBT transformations
    run_dbt_transformations || exit 1
    
    print_section "Pipeline Rebuild Complete"
    echo -e "${GREEN}The entire pipeline has been successfully rebuilt!${NC}"
    echo -e "${YELLOW}To see the status of the pipeline, run: $0 --status${NC}"
}

# Execute main function with passed arguments
main "$@" 