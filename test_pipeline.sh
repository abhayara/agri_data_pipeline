#!/bin/bash

# DEPRECATION NOTICE
echo -e "\033[1;31mDEPRECATION NOTICE\033[0m"
echo -e "\033[1;31m====================\033[0m"
echo -e "\033[1;33mThis script is deprecated and will be removed in a future version.\033[0m"
echo -e "\033[1;33mPlease use the new unified build script instead:\033[0m"
echo -e "\033[1;36m  ./build.sh\033[0m"
echo -e "\033[1;33mFor more information, see BUILD_README.md\033[0m"
echo -e "\033[1;31m====================\033[0m"
echo "Continuing execution in 5 seconds..."
sleep 5

# Load environment variables
source .env

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting full pipeline test...${NC}"

# Step 1: Create the network if it doesn't exist
echo -e "${YELLOW}Step 1: Setting up network...${NC}"
if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
    docker network create ${PROJECT_NAME}-network
    echo -e "${GREEN}Created network ${PROJECT_NAME}-network${NC}"
else
    echo -e "${GREEN}Network ${PROJECT_NAME}-network already exists.${NC}"
fi

# Extra step: Make sure old containers are removed
echo -e "${YELLOW}Cleaning up old containers...${NC}"
docker-compose -f ./docker/streaming/docker-compose.yml down 2>/dev/null || true
docker-compose -f ./docker/kafka/docker-compose.yml down 2>/dev/null || true
docker ps -a | grep 'agri_data' | awk '{print $1}' | xargs docker rm -f 2>/dev/null || true
echo -e "${GREEN}Old containers removed.${NC}"

# Step 2: Start Kafka
echo -e "${YELLOW}Step 2: Starting Kafka...${NC}"
docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env up -d
echo -e "${GREEN}Waiting 30 seconds for Kafka to start...${NC}"
sleep 30

# Step 3: Verify Kafka is running
echo -e "${YELLOW}Step 3: Verifying Kafka is running...${NC}"
if docker ps | grep -q "${PROJECT_NAME}-broker"; then
    echo -e "${GREEN}Kafka is running.${NC}"
else
    echo -e "${RED}Kafka is not running. Exiting.${NC}"
    exit 1
fi

# Step 4: Run the streaming components
echo -e "${YELLOW}Step 4: Starting streaming components...${NC}"
docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d
echo -e "${GREEN}Waiting 20 seconds for streaming components to start...${NC}"
sleep 20

# Step 5: Check if producer completed
echo -e "${YELLOW}Step 5: Checking producer status...${NC}"
PRODUCER_STATUS=$(docker ps -a --filter "name=agri_data_producer" --format "{{.Status}}")
if [[ $PRODUCER_STATUS == *"Exited (0)"* ]]; then
    echo -e "${GREEN}Producer completed successfully.${NC}"
    echo -e "${YELLOW}Producer logs:${NC}"
    docker logs agri_data_producer | tail -n 10
else
    echo -e "${RED}Producer did not complete successfully. Status: $PRODUCER_STATUS${NC}"
    docker logs agri_data_producer | tail -n 10
fi

# Step 6: Check if consumer is processing
echo -e "${YELLOW}Step 6: Checking consumer status...${NC}"
CONSUMER_STATUS=$(docker ps --filter "name=agri_data_consumer" --format "{{.Status}}")
if [[ -n "$CONSUMER_STATUS" ]]; then
    echo -e "${GREEN}Consumer is running.${NC}"
    echo -e "${YELLOW}Consumer logs:${NC}"
    docker logs agri_data_consumer | tail -n 30
else
    echo -e "${RED}Consumer is not running.${NC}"
    docker logs agri_data_consumer | tail -n 30
fi

# Step 7: Test the batch processing
echo -e "${YELLOW}Step 7: Running batch processing (optional - comment out if not testing this part)...${NC}"
# Uncomment this line to test batch processing
# docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env up -d
# python batch_pipeline/export_to_gcs/pipeline.py

# Step 8: Start Airflow and verify (optional)
echo -e "${YELLOW}Step 8: Testing Airflow (optional - comment out if not testing this part)...${NC}"
# Uncomment these lines to test Airflow
# docker-compose -f ./docker/airflow/docker-compose.yml --env-file ./.env up -d
# echo -e "${GREEN}Waiting 30 seconds for Airflow to start...${NC}"
# sleep 30
# Check if Airflow webserver is running
# if docker ps | grep -q "${PROJECT_NAME}-airflow-webserver"; then
#     echo -e "${GREEN}Airflow is running.${NC}"
# else
#     echo -e "${RED}Airflow is not running.${NC}"
# fi

echo -e "${GREEN}Pipeline test completed!${NC}"
echo -e "${YELLOW}You can check logs with: docker logs agri_data_consumer${NC}"
echo -e "${YELLOW}To clean up, run: docker-compose -f ./docker/streaming/docker-compose.yml down && docker-compose -f ./docker/kafka/docker-compose.yml down${NC}" 