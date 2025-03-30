#!/bin/bash

# DEPRECATION NOTICE
echo -e "\033[1;31mDEPRECATION NOTICE\033[0m"
echo -e "\033[1;31m====================\033[0m"
echo -e "\033[1;33mThis script is deprecated and will be removed in a future version.\033[0m"
echo -e "\033[1;33mPlease use the new unified build script instead:\033[0m"
echo -e "\033[1;36m  ./build.sh --clean-only\033[0m"
echo -e "\033[1;33mFor more information, see BUILD_README.md\033[0m"
echo -e "\033[1;31m====================\033[0m"
echo "Continuing execution in 5 seconds..."
sleep 5

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Function to handle Terraform destroy
terraform_destroy() {
    echo -e "${YELLOW}Destroying Terraform resources...${NC}"
    # Check if terraform directory exists
    if [ -d "./terraform" ]; then
        terraform -chdir=terraform destroy -auto-approve
        echo -e "${GREEN}Terraform resources destroyed.${NC}"
    else
        echo -e "${RED}Terraform directory not found. Cannot destroy resources.${NC}"
    fi
}

echo -e "${YELLOW}Stopping and removing all project containers...${NC}"

# Stop all components in reverse order
echo -e "${YELLOW}Stopping streaming components...${NC}"
docker-compose -f ./docker/streaming/docker-compose.yml down
echo -e "${GREEN}Streaming components stopped.${NC}"

echo -e "${YELLOW}Stopping Kafka components...${NC}"
docker-compose -f ./docker/kafka/docker-compose.yml down
echo -e "${GREEN}Kafka components stopped.${NC}"

echo -e "${YELLOW}Stopping Spark components...${NC}"
docker-compose -f ./docker/spark/docker-compose.yml down 2>/dev/null || true
echo -e "${GREEN}Spark components stopped.${NC}"

echo -e "${YELLOW}Stopping Airflow components...${NC}"
docker-compose -f ./docker/airflow/docker-compose.yml down 2>/dev/null || true
echo -e "${GREEN}Airflow components stopped.${NC}"

echo -e "${YELLOW}Stopping PostgreSQL...${NC}"
docker-compose -f ./docker/postgres/docker-compose.yml down 2>/dev/null || true
echo -e "${GREEN}PostgreSQL stopped.${NC}"

echo -e "${YELLOW}Stopping Metabase...${NC}"
docker-compose -f ./docker/metabase/docker-compose.yml down 2>/dev/null || true
echo -e "${GREEN}Metabase stopped.${NC}"

# Remove any lingering containers
echo -e "${YELLOW}Removing any remaining project containers...${NC}"
docker ps -a | grep 'agri_data' | awk '{print $1}' | xargs docker rm -f 2>/dev/null || true

# Clean temp data
echo -e "${YELLOW}Cleaning temporary data...${NC}"
docker run --rm -v /tmp:/tmp busybox rm -rf /tmp/agri_data 2>/dev/null || true
echo -e "${GREEN}Temporary data cleaned.${NC}"

# Ask if user wants to destroy Terraform resources
if [ "$1" = "--destroy-terraform" ]; then
    terraform_destroy
elif [ "$1" != "--no-prompt" ]; then
    echo -e "${YELLOW}Do you want to destroy Terraform resources too? (yes/no)${NC}"
    read -r destroy_terraform
    if [[ "$destroy_terraform" =~ ^[Yy][Ee][Ss]$ ]]; then
        terraform_destroy
    else
        echo -e "${YELLOW}Skipping Terraform resource destruction.${NC}"
    fi
fi

echo -e "${GREEN}Cleanup complete!${NC}" 