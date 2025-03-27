#!/bin/bash

# Project configuration
PROJECT_NAME='agri-data'
EXPORT_TO_BIGQUERY_PIPELINE_UUID='94ab2c7a2aa24bde8e148ef84c88a10f'

# =========================================================
# UTILITY FUNCTIONS
# =========================================================

# Check if the network exists; if not, create it
if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
    docker network create ${PROJECT_NAME}-network
else
    echo "Network ${PROJECT_NAME}-network already exists."
fi

# Function to check if a port is available
check_port() {
    if lsof -Pi :$1 -sTCP:LISTEN -t >/dev/null ; then
        echo "Port $1 is already in use. Please free up this port before continuing."
        return 1
    else
        return 0
    fi
}

# Git helper function
gitting() {
    git add .
    sleep 2
    git commit -m "Update from Local"
    sleep 2
    git push -u origin main
}

# =========================================================
# INFRASTRUCTURE SETUP FUNCTIONS
# =========================================================

# Function to initialize and apply Terraform
terraform-start() {
    terraform -chdir=terraform init
    terraform -chdir=terraform plan
    terraform -chdir=terraform apply
}

# Function to destroy Terraform resources
terraform-destroy() {
    terraform -chdir=terraform destroy
}

# =========================================================
# SERVICE MANAGEMENT FUNCTIONS
# =========================================================

# Function to start Kafka
start-kafka() {
    # Check Kafka-related ports
    check_port 9092 && check_port 2181 && check_port 8081 && check_port 9021 && check_port 8082
    if [ $? -ne 0 ]; then
        echo "Cannot start Kafka due to port conflict."
        return 1
    fi
    docker-compose -f ./docker/kafka/docker-compose.yml up -d
}

# Function to start Spark
start-spark() {
    # Check Spark-related ports
    check_port 8090 && check_port 8091 && check_port 7077 && check_port 8888
    if [ $? -ne 0 ]; then
        echo "Cannot start Spark due to port conflict."
        return 1
    fi
    # Ensure the build script is executable and run it
    chmod +x ./docker/spark/build.sh
    ./docker/spark/build.sh
    # Start Spark containers
    docker-compose -f ./docker/spark/docker-compose.yml up -d
}

# Function to start Airflow
start-airflow() {
   # Check Airflow-related ports
   check_port 8080
   if [ $? -ne 0 ]; then
       echo "Cannot start Airflow due to port conflict."
       return 1
   fi
   docker-compose -f ./docker/airflow/docker-compose.yml up -d
   echo "Airflow is starting up. It may take a moment..."
   echo "You can access the Airflow UI at: http://localhost:8080"
   echo "Username: airflow, Password: airflow"
}

# Function to start Postgres
start-postgres() {
   # Check Postgres-related ports
   check_port 5432 && check_port 5050
   if [ $? -ne 0 ]; then
       echo "Cannot start Postgres due to port conflict."
       return 1
   fi
   docker-compose -f ./docker/postgres/docker-compose.yml up -d
}

# Function to start Metabase
start-metabase() {
   # Check Metabase-related ports
   check_port 3000
   if [ $? -ne 0 ]; then
       echo "Cannot start Metabase due to port conflict."
       return 1
   fi
   docker-compose -f ./docker/metabase/docker-compose.yml up -d
}

# Function to stop Kafka
stop-kafka() {
    docker-compose -f ./docker/kafka/docker-compose.yml down
}

# Function to stop Spark
stop-spark() {
    docker-compose -f ./docker/spark/docker-compose.yml down
}

# Function to stop Airflow
stop-airflow() {
    docker-compose -f ./docker/airflow/docker-compose.yml down
}

# Function to stop Postgres
stop-postgres() {
    docker-compose -f ./docker/postgres/docker-compose.yml down
}

# Function to stop Metabase
stop-metabase() {
    docker-compose -f ./docker/metabase/docker-compose.yml down
}

# Function to stop all services
stop-all-services() {
    stop-airflow
    stop-kafka
    stop-spark
    stop-metabase
}

# =========================================================
# DATA PIPELINE FUNCTIONS
# =========================================================

# Function to start streaming data
stream-data() {
    docker-compose -f ./docker/streaming/docker-compose.yaml up -d
}

# Function to start the streaming pipeline
start-streaming-pipeline() {
    # Start Kafka and Airflow, then begin streaming data
    start-kafka
    if [ $? -ne 0 ]; then
        echo "Failed to start Kafka. Aborting pipeline startup."
        return 1
    fi
    sleep 5
    start-airflow
    if [ $? -ne 0 ]; then
        echo "Failed to start Airflow. Aborting pipeline startup."
        return 1
    fi
    sleep 10
    echo "Triggering data generation DAG in Airflow..."
    curl -X POST "http://localhost:8080/api/v1/dags/agri_data_generator/dagRuns" \
    -H "Content-Type: application/json" \
    -H "Authorization: Basic $(echo -n 'airflow:airflow' | base64)" \
    -d '{"conf": {}}' || echo "Airflow API may not be ready yet. Please trigger the DAG manually from the Airflow UI."
    echo "Starting data streaming..."
    stream-data
}

# Function to stop the streaming pipeline
stop-streaming-pipeline() {
    # Stop Kafka and Airflow
    stop-kafka
    stop-airflow
}

# Function to trigger OLAP transformation pipeline
olap-transformation-pipeline() {
    # Execute the primary pipeline DAG in Airflow
    echo "Triggering the main data pipeline in Airflow..."
    curl -X POST "http://localhost:8080/api/v1/dags/agri_data_pipeline/dagRuns" \
    -H "Content-Type: application/json" \
    -H "Authorization: Basic $(echo -n 'airflow:airflow' | base64)" \
    -d '{"conf": {}}' || echo "Airflow API may not be ready yet. Please trigger the DAG manually from the Airflow UI."
}

# Function to start the batch processing pipeline
start-batch-pipeline() {
    echo "Starting Agricultural Data Batch Pipeline..."

    # Check if Spark is running
    if ! docker ps | grep -q "spark-master"; then
        echo "Starting Spark cluster..."
        start-spark
        if [ $? -ne 0 ]; then
            echo "Failed to start Spark. Aborting batch pipeline startup."
            return 1
        fi
        sleep 10  # Allow time for Spark to start
    else
        echo "Spark cluster is already running"
    fi

    # Check if data exists in GCS bucket
    echo "Checking for data in GCS..."
    if [[ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
        echo "GOOGLE_APPLICATION_CREDENTIALS not set. Using default path: ./gcp-creds.json"
        export GOOGLE_APPLICATION_CREDENTIALS="./gcp-creds.json"
    fi

    # Run the Spark transformation pipeline
    echo "Running Spark transformation pipeline..."
    cd batch_pipeline/export_to_gcs
    python pipeline.py
    if [ $? -ne 0 ]; then
        echo "Failed to run Spark transformation pipeline. Check logs for errors."
        cd ../..
        return 1
    fi
    cd ../..

    # Trigger Airflow DAG for batch processing
    echo "Triggering batch data pipeline in Airflow..."
    curl -X POST "http://localhost:8080/api/v1/dags/batch_data_pipeline/dagRuns" \
    -H "Content-Type: application/json" \
    -H "Authorization: Basic $(echo -n 'airflow:airflow' | base64)" \
    -d '{"conf": {}}' || echo "Airflow API may not be ready yet. Please trigger the DAG manually from the Airflow UI."

    # Run the BigQuery export pipeline
    echo "Running BigQuery export pipeline..."
    cd batch_pipeline/export_to_big_query

    # Export dimension tables
    for dim in farm crop weather soil harvest; do
        echo "Exporting $dim dimension to BigQuery..."
        python data_exporters/${dim}_dim_to_big_query.py
        if [ $? -ne 0 ]; then
            echo "Failed to export $dim dimension. Continuing with other exports..."
        fi
    done

    # Export fact tables
    for fact in production yield sustainability; do
        echo "Exporting $fact facts to BigQuery..."
        python data_exporters/${fact}_facts_to_big_query.py
        if [ $? -ne 0 ]; then
            echo "Failed to export $fact facts. Continuing with other exports..."
        fi
    done

    cd ../..

    echo "Agricultural Data Batch Pipeline completed successfully!"
    
    # Run the OLAP transformation pipeline as a final step
    olap-transformation-pipeline
}

# Function to run dbt transformations
run-dbt-transformations() {
    echo "Running dbt transformations..."
    
    # Check if dbt_cloud.yml exists in ~/.dbt directory
    if [ ! -f ~/.dbt/dbt_cloud.yml ]; then
        echo "dbt Cloud configuration file not found at ~/.dbt/dbt_cloud.yml"
        echo "Please follow the guide at https://docs.getdbt.com/docs/cloud/cloud-cli-installation to configure dbt Cloud CLI"
        echo "After installing, copy your dbt_cloud.yml file to ~/.dbt/ directory"
        return 1
    fi
    
    # Run dbt (use --profiles-dir if needed)
    dbt run
    
    if [ $? -ne 0 ]; then
        echo "Failed to run dbt transformations. Check logs for errors."
        return 1
    fi
    
    echo "dbt transformations completed successfully!"
}

# =========================================================
# MAIN EXECUTION FUNCTION
# =========================================================

# Function to start the entire project
start-project() {
    echo "Checking port availability for all services..."
    # Check all required ports
    check_port 8080 && check_port 9092 && check_port 2181 && check_port 8081 && check_port 9021 && \
    check_port 8082 && check_port 5432 && check_port 5050 && check_port 3000 && \
    check_port 8090 && check_port 8091 && check_port 7077 && check_port 8888
    if [ $? -ne 0 ]; then
        echo "Please resolve port conflicts before starting the project."
        return 1
    fi
    
    echo "Creating Resources in Bigquery..."
    sleep 3
    terraform-start
    echo "Resources created, starting the streaming pipeline..."
    start-streaming-pipeline
    if [ $? -ne 0 ]; then
        echo "Failed to start streaming pipeline."
        return 1
    fi
    echo "Check the Airflow UI at http://localhost:8080 to monitor your DAGs"
    sleep 10
    echo "Waiting 2 mins to get some data, till then check Airflow UI."
    sleep 120
    echo "Starting Batch pipeline..."
    start-spark
    if [ $? -ne 0 ]; then
        echo "Failed to start Spark."
        return 1
    fi
    start-batch-pipeline
    sleep 30
    echo "Batch pipeline execution complete, starting dbt pipeline..."
    run-dbt-transformations
    echo "dbt pipeline execution complete, your data is ready in Bigquery for downstream usecases."
    echo "Start making dashboard in metabase"
    start-metabase
}

# Display welcome message when sourcing the file
echo "==================================================="
echo "Agricultural Data Pipeline Commands Loaded"
echo "==================================================="
echo "To start the entire pipeline: start-project"
echo "For individual components:"
echo "  - start-streaming-pipeline"
echo "  - start-batch-pipeline"
echo "  - run-dbt-transformations"
echo "  - start-metabase"
echo "==================================================="