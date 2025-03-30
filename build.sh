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

# Source the modular functions
if [ -f "./scripts/main.sh" ]; then
    source ./scripts/main.sh
    echo -e "${GREEN}Loaded modular functions from scripts directory${NC}"
else
    echo -e "${RED}ERROR: scripts/main.sh not found. Cannot load modular functions${NC}"
    exit 1
fi

# Function to print section headers
print_section() {
    echo -e "\n${BLUE}===========================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===========================================================${NC}"
}

# Function to destroy all infrastructure
destroy_infrastructure() {
    print_section "Destroying Infrastructure"
    
    # Stop all services in reverse order
    echo -e "${YELLOW}Stopping all running containers...${NC}"
    stop-streaming-pipeline
    stop-spark
    stop-kafka
    
    # Stop any other services
    echo -e "${YELLOW}Removing any remaining project containers...${NC}"
    docker ps -a | grep 'agri_data' | awk '{print $1}' | xargs docker rm -f 2>/dev/null || true
    
    echo -e "${YELLOW}Destroying Terraform resources...${NC}"
    if [ -d "./terraform" ]; then
        terraform -chdir=terraform destroy -auto-approve
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Terraform resources destroyed${NC}"
        else
            echo -e "${RED}Error destroying Terraform resources${NC}"
            return 1
        fi
    else
        echo -e "${RED}Terraform directory not found. Cannot destroy resources${NC}"
    fi
    
    return 0
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
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Terraform initialized${NC}"
        else
            echo -e "${RED}Error initializing Terraform${NC}"
            return 1
        fi
        
        # Apply Terraform configuration
        echo -e "${YELLOW}Applying Terraform configuration...${NC}"
        terraform -chdir=terraform apply -auto-approve
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Terraform resources created${NC}"
        else
            echo -e "${RED}Error applying Terraform configuration${NC}"
            return 1
        fi
    else
        echo -e "${RED}Terraform directory not found. Cannot initialize infrastructure${NC}"
        return 1
    fi
    
    # Install required Python packages
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    pip install -r requirements.txt
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Python dependencies installed${NC}"
    else
        echo -e "${RED}Error installing Python dependencies${NC}"
        return 1
    fi
    
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
    echo -e "  --gcs-status      Check data in GCS bucket"
}

# Function to check GCS bucket status
check_gcs_status() {
    print_section "GCS Bucket Status"
    
    if command -v gsutil &> /dev/null; then
        # Check if the bucket exists
        if gsutil ls gs://${GCS_BUCKET_NAME}/ &>/dev/null; then
            echo -e "${GREEN}GCS bucket '${GCS_BUCKET_NAME}' exists${NC}"
            
            # Check for raw data
            echo -e "${YELLOW}Raw data:${NC}"
            gsutil ls -r gs://${GCS_BUCKET_NAME}/raw/ 2>/dev/null || echo -e "${RED}No raw data found in bucket${NC}"
            
            # Check for OLAP data
            echo -e "${YELLOW}OLAP data:${NC}"
            gsutil ls -r gs://${GCS_BUCKET_NAME}/olap/ 2>/dev/null || echo -e "${RED}No OLAP data found in bucket${NC}"
        else
            echo -e "${RED}GCS bucket '${GCS_BUCKET_NAME}' doesn't exist or isn't accessible${NC}"
            return 1
        fi
    else
        echo -e "${RED}gsutil not available. Cannot check GCS bucket.${NC}"
        echo -e "${YELLOW}Install the Google Cloud SDK or run this in a Cloud Shell environment.${NC}"
        return 1
    fi
    
    return 0
}

# Main execution
main() {
    if [ "$1" == "--help" ]; then
        usage
        exit 0
    fi
    
    if [ "$1" == "--status" ]; then
        status
        exit $?
    fi
    
    if [ "$1" == "--gcs-status" ]; then
        check_gcs_status
        exit $?
    fi
    
    # Check environment
    if ! check-environment; then
        echo -e "${RED}Environment check failed. Please fix the issues before continuing${NC}"
        exit 1
    fi
    
    if [ "$1" == "--clean-only" ]; then
        destroy_infrastructure
        clean_docker_resources
        exit 0
    fi
    
    if [ "$1" == "--streaming-only" ]; then
        # Start Kafka first
        start-kafka || { 
            echo -e "${RED}Failed to start Kafka. Check logs.${NC}"
            exit 1
        }
        # Start streaming pipeline
        start-streaming-pipeline || {
            echo -e "${RED}Failed to start streaming pipeline. Check logs.${NC}"
            exit 1
        }
        # Check status after starting
        check-streaming-status
        exit $?
    fi
    
    if [ "$1" == "--batch-only" ]; then
        # Start Spark first
        start-spark || { 
            echo -e "${RED}Failed to start Spark. Check logs.${NC}"
            exit 1
        }
        # Run batch pipeline
        start-batch-pipeline || {
            echo -e "${RED}Failed to run batch pipeline. Check logs.${NC}"
            exit 1
        }
        exit $?
    fi
    
    if [ "$1" == "--dbt-only" ]; then
        run-dbt || {
            echo -e "${RED}Failed to run DBT transformations. Check logs.${NC}"
            exit 1
        }
        exit $?
    fi
    
    # Default action: rebuild everything
    print_section "Full Pipeline Rebuild"
    
    # 1. Destroy all infrastructure
    destroy_infrastructure || {
        echo -e "${RED}Error destroying infrastructure${NC}"
        exit 1
    }
    
    # 2. Clean Docker resources
    clean_docker_resources
    
    # 3. Initialize infrastructure
    initialize_infrastructure || {
        echo -e "${RED}Error initializing infrastructure${NC}"
        exit 1
    }
    
    # 4. Start Kafka and fix configuration
    echo -e "${YELLOW}Checking and fixing Kafka configuration...${NC}"
    check_fix_kafka_config
    
    # 5. Start Kafka
    start-kafka || {
        echo -e "${RED}Failed to start Kafka. Check logs.${NC}"
        exit 1
    }
    
    # 6. Start Spark and fix configuration
    echo -e "${YELLOW}Checking and fixing GCS configuration...${NC}"
    check_fix_gcs_config
    
    # 7. Start Spark
    start-spark || {
        echo -e "${RED}Failed to start Spark. Check logs.${NC}"
        exit 1
    }
    
    # 8. Start streaming pipeline
    start-streaming-pipeline || {
        echo -e "${RED}Failed to start streaming pipeline. Check logs.${NC}"
        exit 1
    }
    
    # 9. Run batch pipeline
    echo -e "${YELLOW}Waiting for data to be streamed to GCS...${NC}"
    sleep 60  # Wait for some data to be produced and consumed
    
    start-batch-pipeline || {
        echo -e "${YELLOW}Warning: Batch pipeline encountered errors. Check logs.${NC}"
    }
    
    # 10. Run DBT transformations
    run-dbt || {
        echo -e "${YELLOW}Warning: DBT transformations encountered errors. Check logs.${NC}"
    }
    
    print_section "Pipeline Rebuild Complete"
    echo -e "${GREEN}The entire pipeline has been successfully rebuilt!${NC}"
    
    # Check final status
    status
    
    # Check GCS bucket status
    check_gcs_status || true
    
    echo -e "${YELLOW}To see the detailed status of the pipeline, run: $0 --status${NC}"
}

# Execute main function with passed arguments
main "$@" 