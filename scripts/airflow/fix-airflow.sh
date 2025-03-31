#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;36m'
NC='\033[0m' # No Color

# Project settings
PROJECT_NAME="agri_data_pipeline"

# Airflow settings
AIRFLOW_DB_PORT=5433
AIRFLOW_DB_USER=postgres
AIRFLOW_DB_PASSWORD=postgres
AIRFLOW_DB_NAME=airflow
AIRFLOW_PORT=8080
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW__CORE__FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=
AIRFLOW__WEBSERVER__SECRET_KEY=a25mO1S5uYzj9n9+4oaMOxiXIc2cI+XL

# Function to display section headers
print_section() {
    echo -e "\n${BLUE}===========================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===========================================================${NC}"
}

# Function to clean existing Airflow resources
clean_airflow() {
    print_section "Cleaning Airflow resources"

    # Stop and remove Airflow containers
    echo -e "${YELLOW}Stopping any running Airflow containers...${NC}"
    docker ps -a | grep "${PROJECT_NAME}-airflow" | awk '{print $1}' | xargs -r docker rm -f

    # Remove Airflow volumes
    echo -e "${YELLOW}Removing Airflow volumes...${NC}"
    docker volume ls | grep "airflow" | awk '{print $2}' | xargs -r docker volume rm

    # Clean up log directories
    echo -e "${YELLOW}Cleaning Airflow log directories...${NC}"
    sudo rm -rf ./docker/airflow/logs
    mkdir -p ./docker/airflow/logs ./docker/airflow/dags ./docker/airflow/plugins ./docker/airflow/config
    sudo chmod -R 777 ./docker/airflow/logs ./docker/airflow/dags ./docker/airflow/plugins ./docker/airflow/config

    echo -e "${GREEN}Airflow resources cleaned successfully${NC}"
}

# Function to create the Airflow environment file
create_env_file() {
    print_section "Creating Airflow .env file"

    cat > ./docker/airflow/.env << EOF
# Airflow specific variables
PROJECT_NAME=${PROJECT_NAME}
AIRFLOW_UID=50000
AIRFLOW_DB_HOST=postgres
AIRFLOW_DB_PORT=${AIRFLOW_DB_PORT}
AIRFLOW_DB_USER=${AIRFLOW_DB_USER}
AIRFLOW_DB_PASSWORD=${AIRFLOW_DB_PASSWORD}
AIRFLOW_DB_NAME=${AIRFLOW_DB_NAME}
AIRFLOW_PORT=${AIRFLOW_PORT}
_AIRFLOW_WWW_USER_USERNAME=${_AIRFLOW_WWW_USER_USERNAME}
_AIRFLOW_WWW_USER_PASSWORD=${_AIRFLOW_WWW_USER_PASSWORD}
AIRFLOW__CORE__FERNET_KEY=${AIRFLOW__CORE__FERNET_KEY}
AIRFLOW__WEBSERVER__SECRET_KEY=${AIRFLOW__WEBSERVER__SECRET_KEY}
EOF

    echo -e "${GREEN}Created .env file at ./docker/airflow/.env${NC}"
}

# Function to start Airflow
start_airflow() {
    print_section "Starting Airflow"

    # Start Airflow services directly without creating a custom network
    echo -e "${YELLOW}Starting Airflow services...${NC}"
    docker-compose -f ./docker/airflow/docker-compose.yml up -d
    
    # Wait for Airflow to be ready
    echo -e "${YELLOW}Waiting for Airflow to be ready...${NC}"
    attempt=0
    max_attempts=60
    while [ $attempt -lt $max_attempts ]; do
        attempt=$((attempt+1))
        echo -e "${YELLOW}Checking Airflow webserver status ($attempt/$max_attempts)...${NC}"
        
        if curl -s http://localhost:${AIRFLOW_PORT}/health &>/dev/null; then
            echo -e "${GREEN}✅ Airflow is running and healthy!${NC}"
            echo -e "${GREEN}Airflow UI is accessible at: http://localhost:${AIRFLOW_PORT}${NC}"
            echo -e "${GREEN}Username: ${_AIRFLOW_WWW_USER_USERNAME}${NC}"
            echo -e "${GREEN}Password: ${_AIRFLOW_WWW_USER_PASSWORD}${NC}"
            return 0
        fi
        sleep 10
    done
    
    echo -e "${RED}Timed out waiting for Airflow to start. Please check the logs.${NC}"
    docker-compose -f ./docker/airflow/docker-compose.yml logs --tail=100
    return 1
}

# Main execution
main() {
    clean_airflow
    create_env_file
    start_airflow
}

# Run the main function
main 