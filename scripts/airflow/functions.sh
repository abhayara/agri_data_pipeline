#!/bin/bash
# Airflow functions for the agriculture data pipeline

# Start Airflow
start-airflow() {
    print_section "Starting Airflow services..."
    
    # Check environment
    check-environment || {
        echo -e "${RED}Environment check failed. Please fix the issues before continuing${NC}"
        return 1
    }
    
    # Generate Fernet key if not already set
    if [ -z "${AIRFLOW__CORE__FERNET_KEY}" ]; then
        export AIRFLOW__CORE__FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
        echo -e "${GREEN}Generated Fernet key: ${AIRFLOW__CORE__FERNET_KEY}${NC}"
    fi
    
    # Generate webserver secret key if not already set
    if [ -z "${AIRFLOW__WEBSERVER__SECRET_KEY}" ]; then
        export AIRFLOW__WEBSERVER__SECRET_KEY=$(python3 -c "import os; import base64; print(base64.b64encode(os.urandom(16)).decode())")
        echo -e "${GREEN}Generated webserver secret key: ${AIRFLOW__WEBSERVER__SECRET_KEY}${NC}"
    fi
    
    # Make sure postgres directory exists and has proper permissions
    echo -e "${YELLOW}Ensuring Airflow directories exist with proper permissions...${NC}"
    mkdir -p ./docker/airflow/logs ./docker/airflow/dags ./docker/airflow/plugins ./docker/airflow/config
    
    # Fix permissions issue by removing existing logs directory if it has permission problems
    if [ ! -w "./docker/airflow/logs" ] || ! chmod -R 777 ./docker/airflow/logs 2>/dev/null; then
        echo -e "${YELLOW}Permission issues with logs directory. Recreating it...${NC}"
        sudo rm -rf ./docker/airflow/logs
        mkdir -p ./docker/airflow/logs
    fi
    
    # Now set permissions on all directories
    sudo chmod -R 777 ./docker/airflow/logs ./docker/airflow/dags ./docker/airflow/plugins ./docker/airflow/config
    
    # Create a temporary .env file with the correct environment variables
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
    
    # Start Airflow services directly with docker-compose
    echo -e "${YELLOW}Starting Airflow containers...${NC}"
    docker-compose -f ./docker/airflow/docker-compose.yml up -d
    
    # Wait for Airflow to be ready
    echo -e "${YELLOW}Waiting for Airflow to be ready...${NC}"
    attempt=0
    max_attempts=60  # 10 minutes total
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
    
    # Don't return failure here, just warn the user
    echo -e "${YELLOW}⚠️ Airflow services may not be fully operational. Continuing with other components...${NC}"
    return 0  # Return success anyway so the pipeline can continue
}

# Stop Airflow
stop-airflow() {
    print_section "Stopping Airflow services..."
    
    # Try graceful shutdown first
    echo -e "${YELLOW}Attempting graceful shutdown of Airflow...${NC}"
    docker-compose -f ./docker/airflow/docker-compose.yml down --timeout 60
    
    # Check if containers are still running
    if docker ps | grep -q "${PROJECT_NAME}-airflow"; then
        echo -e "${YELLOW}Some Airflow containers still running. Forcing removal...${NC}"
        docker ps -a | grep "${PROJECT_NAME}-airflow" | awk '{print $1}' | xargs -r docker rm -f
    fi
    
    # Check if any Airflow containers are still running after forced removal
    if docker ps | grep -q "${PROJECT_NAME}-airflow"; then
        echo -e "${RED}Failed to stop all Airflow services.${NC}"
        return 1
    else
        echo -e "${GREEN}All Airflow services stopped.${NC}"
    fi
    
    return 0
}

# Check Airflow status
check-airflow-status() {
    print_section "Checking Airflow status..."
    
    # Check if containers are running
    local airflow_containers=$(docker ps --filter "name=airflow" --format "{{.Names}}: {{.Status}}")
    
    if [ -z "$airflow_containers" ]; then
        echo -e "${RED}❌ No Airflow containers are running.${NC}"
        return 1
    else
        echo -e "${GREEN}Airflow containers:${NC}"
        echo "$airflow_containers" | while read container; do
            echo -e "✅ $container"
        done
        
        # Check Airflow webserver health
        if curl -s http://localhost:${AIRFLOW_PORT}/health &>/dev/null; then
            echo -e "${GREEN}✅ Airflow webserver is healthy${NC}"
            echo -e "${GREEN}Airflow UI is accessible at: http://localhost:${AIRFLOW_PORT}${NC}"
        else
            echo -e "${RED}❌ Airflow webserver is not healthy${NC}"
        fi
        
        # List DAGs
        echo -e "${YELLOW}Listing Airflow DAGs:${NC}"
        docker exec -it airflow-airflow-webserver-1 airflow dags list 2>/dev/null || echo -e "${RED}Failed to list DAGs${NC}"
    fi
    
    return 0
}

# Trigger a DAG run
trigger-dag() {
    local dag_id=$1
    
    if [ -z "$dag_id" ]; then
        echo -e "${RED}Error: No DAG ID provided.${NC}"
        echo -e "${YELLOW}Usage: trigger-dag <dag_id>${NC}"
        return 1
    fi
    
    print_section "Triggering DAG run: $dag_id"
    
    # Check if DAG exists
    if ! docker exec -it airflow-airflow-webserver-1 airflow dags list | grep -q "$dag_id"; then
        echo -e "${RED}Error: DAG '$dag_id' not found.${NC}"
        return 1
    fi
    
    # Trigger the DAG
    docker exec -it airflow-airflow-webserver-1 airflow dags trigger "$dag_id"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ DAG '$dag_id' triggered successfully.${NC}"
        echo -e "${YELLOW}Check the Airflow UI for run status: http://localhost:${AIRFLOW_PORT}/dags/$dag_id${NC}"
    else
        echo -e "${RED}❌ Failed to trigger DAG '$dag_id'.${NC}"
        return 1
    fi
    
    return 0
}

# Update the main.sh script to include airflow functions
update_main_sh() {
    print_section "Adding Airflow functions to main.sh"
    
    # Check if the script already includes airflow functions
    if grep -q "./scripts/airflow/functions.sh" ./scripts/main.sh; then
        echo -e "${GREEN}Airflow functions already included in main.sh${NC}"
        return 0
    fi
    
    # Add airflow functions to the MODULES array
    sed -i '/MODULES=(/a \    "./scripts/airflow/functions.sh"' ./scripts/main.sh
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Updated main.sh to include Airflow functions${NC}"
    else
        echo -e "${RED}Failed to update main.sh${NC}"
        return 1
    fi
    
    return 0
}

# Create a DAG file
create-dag() {
    local dag_name=$1
    local schedule="${2:-@daily}"
    
    if [ -z "$dag_name" ]; then
        echo -e "${RED}Error: No DAG name provided.${NC}"
        echo -e "${YELLOW}Usage: create-dag <dag_name> [schedule]${NC}"
        return 1
    fi
    
    print_section "Creating DAG: $dag_name"
    
    # Create the DAG directory
    mkdir -p ./docker/airflow/dags
    
    # Create the DAG file
    cat > ./docker/airflow/dags/${dag_name}.py << EOF
"""
${dag_name}
DAG for the agriculture data pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '${dag_name}',
    default_args=default_args,
    description='Agriculture Data Pipeline DAG',
    schedule_interval='${schedule}',
    start_date=datetime(2025, 3, 30),
    catchup=False,
    tags=['agriculture', 'data', 'pipeline'],
) as dag:
    
    # Define tasks here
    task_start = BashOperator(
        task_id='start',
        bash_command='echo "Starting ${dag_name} at $(date)"',
    )
    
    # Add your tasks here
    
    task_end = BashOperator(
        task_id='end',
        bash_command='echo "Completed ${dag_name} at $(date)"',
    )
    
    # Define task dependencies
    task_start >> task_end
EOF
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Created DAG file: ./docker/airflow/dags/${dag_name}.py${NC}"
    else
        echo -e "${RED}Failed to create DAG file${NC}"
        return 1
    fi
    
    return 0
}

# Define the print_section function here to avoid the command not found error
print_section() {
    echo -e "\n${BLUE}===========================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===========================================================${NC}"
} 