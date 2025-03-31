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
    
    # Stop airflow with more robust handling
    echo -e "${YELLOW}Stopping Airflow services with enhanced error handling...${NC}"
    if docker ps | grep -q "${PROJECT_NAME}-airflow"; then
        echo -e "${YELLOW}Attempting to stop Airflow services gracefully...${NC}"
        stop-airflow || {
            echo -e "${YELLOW}Failed to stop Airflow gracefully. Forcing removal...${NC}"
            docker ps -a | grep "${PROJECT_NAME}-airflow" | awk '{print $1}' | xargs -r docker rm -f
        }
    else
        echo -e "${GREEN}No Airflow services running. Nothing to stop.${NC}"
    fi
    
    # Stop any other services
    echo -e "${YELLOW}Removing any remaining project containers...${NC}"
    docker ps -a | grep 'agri_data' | awk '{print $1}' | xargs -r docker rm -f 2>/dev/null || true
    
    echo -e "${YELLOW}Destroying Terraform resources...${NC}"
    if [ -d "./terraform" ]; then
        # First, check if the BigQuery dataset configuration has delete_contents_on_destroy set
        if ! grep -q "delete_contents_on_destroy = true" ./terraform/main.tf; then
            echo -e "${YELLOW}Setting delete_contents_on_destroy=true for BigQuery dataset...${NC}"
            # Add delete_contents_on_destroy to BigQuery dataset if not already present
            sed -i '/resource "google_bigquery_dataset" "bigquery-dataset" {/,/}/s/}/  delete_contents_on_destroy = true\n}/' ./terraform/main.tf
            
            # Apply the change first
            echo -e "${YELLOW}Applying configuration update...${NC}"
            terraform -chdir=terraform apply -auto-approve
            if [ $? -ne 0 ]; then
                echo -e "${RED}Error updating Terraform configuration${NC}"
                # Continue anyway to try the destroy
            fi
        fi
        
        # Now run the destroy command
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
    echo -e "  --dbt-seeds       Only load the DBT seed data"
    echo -e "  --dbt-docs-status Check the status of the DBT documentation server"
    echo -e "  --gcs-status      Check data in GCS bucket"
    echo -e "  --rebuild-streaming Rebuild the streaming pipeline with dynamic broker IP"
}

# Function to check GCS bucket status
check_gcs_status() {
    print_section "GCS Bucket Status"
    
    if command -v gsutil &> /dev/null; then
        # Check if the bucket exists
        if gsutil ls gs://${GCS_BUCKET_NAME}/ &>/dev/null; then
            echo -e "${GREEN}GCS bucket '${GCS_BUCKET_NAME}' exists${NC}"
            
            # Check for raw data (just show status, not content)
            if gsutil ls -r gs://${GCS_BUCKET_NAME}/raw/ &>/dev/null && [ "$(gsutil ls -r gs://${GCS_BUCKET_NAME}/raw/ | wc -l)" -gt 0 ]; then
                echo -e "${GREEN}✅ Raw data exists in GCS bucket${NC}"
            else
                echo -e "${RED}❌ No raw data found in bucket${NC}"
            fi
            
            # Check for OLAP data (just show status, not content)
            if gsutil ls -r gs://${GCS_BUCKET_NAME}/olap/ &>/dev/null && [ "$(gsutil ls -r gs://${GCS_BUCKET_NAME}/olap/ | wc -l)" -gt 0 ]; then
                echo -e "${GREEN}✅ OLAP data exists in GCS bucket${NC}"
            else
                echo -e "${RED}❌ No OLAP data found in bucket${NC}"
            fi
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

# Function to rebuild streaming pipeline with dynamic broker IP
rebuild_streaming_pipeline() {
    print_section "Rebuilding Streaming Pipeline"
    
    # First stop the streaming pipeline
    echo -e "${YELLOW}Stopping streaming pipeline...${NC}"
    stop-streaming-pipeline
    
    # Update broker hostname resolution
    echo -e "${YELLOW}Updating broker hostname resolution...${NC}"
    ensure_broker_hostname_resolution
    
    # Start the streaming pipeline with updated configuration
    echo -e "${YELLOW}Starting streaming pipeline with updated broker configurations...${NC}"
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d --build
    
    # Check status
    echo -e "${YELLOW}Checking streaming pipeline status...${NC}"
    sleep 15
    check-streaming-status
    
    echo -e "${GREEN}Streaming pipeline rebuilt with dynamic broker IP resolution${NC}"
}

# Function to run dbt seeds first, then the dbt models
run_dbt_with_seeds() {
    print_section "Running DBT Seeds and Models"
    
    echo -e "${YELLOW}Loading seed data into BigQuery...${NC}"
    run-dbt-seeds || {
        echo -e "${RED}Failed to load seed data. DBT models may fail.${NC}"
        # Continue despite error, as we'll try to run models anyway
    }
    
    echo -e "${YELLOW}Running DBT models on BigQuery data...${NC}"
    run-dbt
    
    return $?
}

# Function to wait for raw data in GCS before starting batch processing
wait_for_gcs_data() {
    print_section "Waiting for Raw Data in GCS"
    
    MAX_ATTEMPTS=30  # 5 minutes total wait time
    ATTEMPT=1
    
    echo "Checking for raw data in GCS bucket '${GCS_BUCKET_NAME}'..."
    
    while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
        echo "Check attempt $ATTEMPT of $MAX_ATTEMPTS (waiting for data in GCS)..."
        
        if gsutil ls "gs://${GCS_BUCKET_NAME}/raw/" &>/dev/null && 
           gsutil ls "gs://${GCS_BUCKET_NAME}/raw/agri_data/" &>/dev/null && 
           [ "$(gsutil ls -r "gs://${GCS_BUCKET_NAME}/raw/agri_data/" | wc -l)" -gt 0 ]; then
            echo -e "${GREEN}✅ Raw data found in GCS bucket! Proceeding with batch processing.${NC}"
            return 0
        fi
        
        echo "No raw data found yet. Waiting 10 seconds before checking again..."
        sleep 10
        ATTEMPT=$((ATTEMPT + 1))
    done
    
    echo -e "${YELLOW}⚠️ Timeout reached while waiting for raw data. You may need to check the streaming pipeline.${NC}"
    echo -e "${YELLOW}Proceeding with batch processing anyway, but it may not find any data to process.${NC}"
    return 1
}

# Function to wait for Spark jobs to complete before starting DBT
wait_for_spark_job() {
    print_section "Waiting for Spark Batch Processing"
    
    MAX_ATTEMPTS=20  # ~5 minutes total wait time
    ATTEMPT=1
    
    echo "Checking for Spark batch processing completion..."
    
    while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
        echo "Check attempt $ATTEMPT of $MAX_ATTEMPTS (waiting for Spark job)..."
        
        # Check if any data has been processed to OLAP folder
        if gsutil ls "gs://${GCS_BUCKET_NAME}/olap/" &>/dev/null && 
           [ "$(gsutil ls -r "gs://${GCS_BUCKET_NAME}/olap/" | wc -l)" -gt 0 ]; then
            echo -e "${GREEN}✅ Spark job completed! OLAP data found in GCS bucket. Proceeding with DBT.${NC}"
            return 0
        fi
        
        # Also check if the Spark job has completed by looking at the container logs
        if docker logs agri_data_pipeline-spark-master 2>&1 | grep -q "Batch Processing completed successfully"; then
            echo -e "${GREEN}✅ Spark job completed successfully! Proceeding with DBT.${NC}"
            return 0
        fi
        
        echo "Spark processing still ongoing. Waiting 15 seconds before checking again..."
        sleep 15
        ATTEMPT=$((ATTEMPT + 1))
    done
    
    echo -e "${YELLOW}⚠️ Timeout reached while waiting for Spark job. Proceeding with DBT anyway.${NC}"
    return 1
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
    
    if [ "$1" == "--rebuild-streaming" ]; then
        rebuild_streaming_pipeline
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
        
        # Ensure broker hostname resolution
        ensure_broker_hostname_resolution
        
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
        # Wait for raw data in GCS
        wait_for_gcs_data
        
        # Start batch processing
        start-batch-processing || {
            echo -e "${RED}Failed to start batch processing. Check logs.${NC}"
            exit 1
        }
        exit $?
    fi
    
    if [ "$1" == "--dbt-only" ]; then
        # Wait for Spark job to complete
        wait_for_spark_job
        
        # Start DBT transformations
        start-dbt-transformations || {
            echo -e "${RED}Failed to start DBT transformations. Check logs.${NC}"
            exit 1
        }
        exit $?
    fi
    
    if [ "$1" == "--dbt-seeds" ]; then
        run-dbt-seeds || {
            echo -e "${RED}Failed to load DBT seed data. Check logs.${NC}"
            exit 1
        }
        exit $?
    fi
    
    if [ "$1" == "--dbt-docs-status" ]; then
        check-dbt-docs-status
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
    
    # 4. Start Kafka
    start-kafka || {
        echo -e "${RED}Failed to start Kafka. Check logs.${NC}"
        exit 1
    }
    
    # 5.5 Ensure broker hostname resolution
    echo -e "${YELLOW}Ensuring dynamic broker hostname resolution...${NC}"
    ensure_broker_hostname_resolution
    
    # 6. Start Spark and fix configuration
    echo -e "${YELLOW}Checking and fixing GCS configuration...${NC}"
    check_fix_gcs_config
    
    # 7. Start Spark
    start-spark || {
        echo -e "${RED}Failed to start Spark. Check logs.${NC}"
        exit 1
    }
    
    # 8. Start Airflow with more robust error handling
    echo -e "${YELLOW}Starting Airflow with enhanced error handling...${NC}"
    start-airflow || {
        echo -e "${RED}Failed to start Airflow on first attempt. Cleaning up and retrying...${NC}"
        # Force stop any running Airflow containers
        docker ps -a | grep "${PROJECT_NAME}-airflow" | awk '{print $1}' | xargs -r docker rm -f 2>/dev/null || true
        # Clean up Airflow volumes
        docker volume ls | grep "airflow" | awk '{print $2}' | xargs -r docker volume rm 2>/dev/null || true
        # Try starting Airflow again
        echo -e "${YELLOW}Retrying Airflow startup...${NC}"
        start-airflow || {
            echo -e "${RED}Failed to start Airflow after retry. Continuing with other components...${NC}"
            # Continue despite error, as we want other components to work
            echo -e "${YELLOW}Continuing with other components...${NC}"
        }
    }
    
    # 9. Start streaming pipeline
    start-streaming-pipeline || {
        echo -e "${RED}Failed to start streaming pipeline. Check logs.${NC}"
        exit 1
    }
    
    # 10. Wait for raw data to be uploaded to GCS
    echo -e "${YELLOW}Waiting for raw data to be uploaded to GCS...${NC}"
    wait_for_gcs_data
    
    # 11. Start batch processing
    start-batch-processing || {
        echo -e "${RED}Failed to start batch processing. Check logs.${NC}"
        exit 1
    }
    
    # 12. Wait for Spark job to complete
    echo -e "${YELLOW}Waiting for Spark job to complete...${NC}"
    wait_for_spark_job
    
    # 13. Start DBT transformations
    start-dbt-transformations || {
        echo -e "${RED}Failed to start DBT transformations. Check logs.${NC}"
        exit 1
    }
    
    # 14. Verify DBT documentation status
    echo -e "${YELLOW}Verifying DBT documentation status...${NC}"
    check-dbt-docs-status || {
        echo -e "${YELLOW}DBT documentation server is not running. Attempting to start it...${NC}"
        serve-dbt-docs || echo -e "${RED}Failed to start DBT documentation server. Check logs.${NC}"
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

status() {
    print_section "Agricultural Data Pipeline Status"
    
    # Check Kafka status
    echo -e "${YELLOW}Checking Kafka status...${NC}"
    check-kafka-status || echo -e "${RED}Kafka check failed${NC}"
    
    # Check Streaming status
    echo -e "${YELLOW}Checking streaming pipeline status...${NC}"
    check-streaming-status || echo -e "${RED}Streaming pipeline check failed${NC}"
    
    # Check GCS Storage status
    echo -e "${YELLOW}Checking GCS storage status...${NC}"
    check_gcs_status || echo -e "${RED}GCS storage check failed${NC}"
    
    # Check Spark status
    echo -e "${YELLOW}Checking Spark status...${NC}"
    check-spark-status || echo -e "${RED}Spark check failed${NC}"
    
    # Check Airflow status
    echo -e "${YELLOW}Checking Airflow status...${NC}"
    check-airflow-status || echo -e "${RED}Airflow check failed${NC}"
    
    # Check Batch Processing status
    echo -e "${YELLOW}Checking batch processing status...${NC}"
    check-batch-status || echo -e "${RED}Batch processing check failed${NC}"
    
    # Check DBT status
    echo -e "${YELLOW}Checking DBT status...${NC}"
    verify-dbt || echo -e "${RED}DBT check failed${NC}"
    
    # Check DBT documentation server status
    echo -e "${YELLOW}Checking DBT documentation server status...${NC}"
    check-dbt-docs-status || echo -e "${YELLOW}DBT documentation server is not running. Run 'serve-dbt-docs' to start it.${NC}"
    
    # Check Metabase status
    echo -e "${YELLOW}Checking Metabase status...${NC}"
    check-metabase-status || echo -e "${RED}Metabase check failed${NC}"
    
    print_section "Status Check Complete"
} 