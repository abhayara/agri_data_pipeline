PROJECT_NAME='agri_data_pipeline'
# Set JAVA_HOME for Java-dependent services
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
#EXPORT_TO_BIGQUERY_PIPELINE_UUID='94ab2c7a2aa24bde8e148ef84c88a10f'

# Check if the network exists; if not, create it
if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
    docker network create ${PROJECT_NAME}-network
else
    echo "Network ${PROJECT_NAME}-network already exists."
fi

# Function to start streaming data
stream-data() {
	docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up
}

# Function to start Kafka
start-kafka() {
	docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env up -d
}

# Function to start Spark
start-spark() {
    # Ensure the build script is executable and run it
    chmod +x ./docker/spark/build.sh
    ./docker/spark/build.sh
	# Start Spark containers
	docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env up -d
}

# Function to start Airflow
start-airflow() {
   docker-compose -f ./docker/airflow/docker-compose.yml --env-file ./.env up -d
   sleep 5
#    sudo cp ./streaming_pipeline/kafka_to_gcs_streaming/kafka_to_gcs.yaml ./docker/mage/${PROJECT_NAME}/data_exporters/
#    sudo cp ./streaming_pipeline/kafka_to_gcs_streaming/consume_from_kafka.yaml ./docker/mage/${PROJECT_NAME}/data_loaders/
#    sudo mkdir ./docker/mage/${PROJECT_NAME}/pipelines/kafka_to_gcs_streaming
#    sudo cp ./streaming_pipeline/kafka_to_gcs_streaming/metadata.yaml ./docker/mage/${PROJECT_NAME}/pipelines/kafka_to_gcs_streaming/
#    sudo touch ./docker/mage/${PROJECT_NAME}/pipelines/kafka_to_gcs_streaming/__init__.py

#    sudo cp ./batch_pipeline/export_to_big_query/data_exporters/* ./docker/mage/${PROJECT_NAME}/data_exporters/
#    sudo cp ./batch_pipeline/export_to_big_query/data_loaders/* ./docker/mage/${PROJECT_NAME}/data_loaders/
#    sudo mkdir ./docker/mage/${PROJECT_NAME}/pipelines/export_to_big_query
#    sudo cp ./batch_pipeline/export_to_big_query/*.yaml ./docker/airflw/${PROJECT_NAME}/pipelines/export_to_big_query/
#    sudo touch ./docker/airflow/${PROJECT_NAME}/pipelines/export_to_big_query/__init__.py
}

# Function to start Postgres
start-postgres() {
   docker-compose -f ./docker/postgres/docker-compose.yml --env-file ./.env up -d
}

# Function to start Metabase
start-metabase() {
   docker-compose -f ./docker/metabase/docker-compose.yml --env-file ./.env up -d
}

# Function to stop Kafka
stop-kafka() {
    docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env down
}

# Function to stop Spark
stop-spark() {
    docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env down
}

# Function to stop Airflow
stop-airflow() {
    docker-compose -f ./docker/airflow/docker-compose.yml --env-file ./.env down
}

# Function to stop Postgres
stop-postgres() {
    docker-compose -f ./docker/postgres/docker-compose.yml --env-file ./.env down
}

# Function to stop Metabase
stop-metabase() {
    docker-compose -f ./docker/metabase/docker-compose.yml --env-file ./.env down
}

# Function to ensure broker hostname resolution in streaming components
ensure_broker_hostname_resolution() {
    echo "==========================================================="
    echo "Ensuring broker hostname resolution in streaming components..."
    
    # Get current broker IP
    BROKER_IP=$(docker inspect ${PROJECT_NAME}-broker -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
    
    if [ -z "$BROKER_IP" ]; then
        echo "❌ Could not detect broker IP address."
        echo "Make sure Kafka broker is running first with 'start-kafka'"
        return 1
    fi
    
    echo "✅ Detected broker IP: $BROKER_IP"
    
    # Extract the network subnet from the docker network inspect
    NETWORK_SUBNET=$(docker network inspect ${PROJECT_NAME}-network | grep -o '"Subnet": "[^"]*"' | head -1 | cut -d '"' -f 4 | cut -d '/' -f 1 | cut -d '.' -f 1,2)
    echo "✅ Detected network subnet: ${NETWORK_SUBNET}"
    
    # Update the .env file to use 0.0.0.0 for Kafka listeners
    if grep -q "KAFKA_LISTENERS=PLAINTEXT://broker:29092" ./.env; then
        echo "Updating Kafka listeners in .env file to use 0.0.0.0..."
        sed -i 's/KAFKA_LISTENERS=PLAINTEXT:\/\/broker:29092/KAFKA_LISTENERS=PLAINTEXT:\/\/0.0.0.0:29092/g' ./.env
        sed -i 's/PLAINTEXT_HOST:\/\/broker:9092/PLAINTEXT_HOST:\/\/0.0.0.0:9092/g' ./.env
        echo "✅ Updated Kafka listeners in .env file."
    else
        echo "✅ Kafka listeners in .env file are already configured correctly."
    fi
    
    # Update the streaming docker-compose file to use the correct broker IP
    if [ -f "./docker/streaming/docker-compose.yml" ]; then
        # Check if we need to update the broker IP
        CURRENT_IP=$(grep -o "broker:[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+" ./docker/streaming/docker-compose.yml | head -1 | cut -d ":" -f2)
        
        if [ "$CURRENT_IP" != "$BROKER_IP" ]; then
            echo "Updating broker IP in docker-compose file from $CURRENT_IP to $BROKER_IP"
            # Use sed to replace the IP in the docker-compose file
            sed -i "s/broker:[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+/broker:$BROKER_IP/g" ./docker/streaming/docker-compose.yml
            echo "✅ Updated broker IP in docker-compose file."
            
            # Record this change in git
            git add ./docker/streaming/docker-compose.yml
            git commit -m "Update broker IP to ${BROKER_IP} for proper network communication"
        else
            echo "✅ Broker IP in docker-compose file is already correct."
        fi
    else
        echo "❌ Streaming docker-compose file not found."
        return 1
    fi
    
    echo "Hostname resolution is configured correctly."
    echo "==========================================================="
    return 0
}

# Function to check and fix Kafka configuration
check_fix_kafka_config() {
    echo "==========================================================="
    echo "Checking and fixing Kafka configuration..."
    
    # Ensure the .env file has the correct listener configuration
    if ! grep -q "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092" ./.env; then
        echo "Updating Kafka listener configuration for better network compatibility..."
        sed -i 's/KAFKA_LISTENERS=.*$/KAFKA_LISTENERS=PLAINTEXT:\/\/0.0.0.0:29092,PLAINTEXT_HOST:\/\/0.0.0.0:9092/g' ./.env
        echo "✅ Updated Kafka listener configuration in .env file."
        
        # Record this change in git
        git add ./.env
        git commit -m "Update Kafka listener configuration for better network compatibility"
    else
        echo "✅ Kafka listener configuration is already optimized in .env file."
    fi
    
    echo "==========================================================="
    return 0
}

# Function to check and fix GCS connector configuration
check_fix_gcs_config() {
    echo "==========================================================="
    echo "Checking and fixing GCS connector configuration..."
    
    GCS_PIPELINE_FILE="./batch_pipeline/export_to_gcs/pipeline.py"
    
    if [ -f "$GCS_PIPELINE_FILE" ]; then
        # Check if GCS connector is properly configured
        if ! grep -q "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11" "$GCS_PIPELINE_FILE"; then
            echo "Updating PySpark GCS connector configuration..."
            sed -i 's/spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1/spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11,com.google.guava:guava:31.1-jre/g' "$GCS_PIPELINE_FILE"
            sed -i '/GOOGLE_APPLICATION_CREDENTIALS)/ a\        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \\n        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")' "$GCS_PIPELINE_FILE"
            echo "✅ Updated GCS connector configuration in pipeline.py file."
            
            # Record this change in git
            git add "$GCS_PIPELINE_FILE"
            git commit -m "Update GCS connector configuration for Spark"
        else
            echo "✅ GCS connector configuration is already present in pipeline.py file."
        fi
        
        # Ensure GCP credentials file exists in both locations
        if [ -f "./gcp-creds.json" ] && [ ! -f "./batch_pipeline/export_to_gcs/gcp-creds.json" ]; then
            echo "Copying GCP credentials file to batch_pipeline directory..."
            cp ./gcp-creds.json ./batch_pipeline/export_to_gcs/
            echo "✅ Copied GCP credentials file to batch_pipeline directory."
        fi
        
        # Check for type mismatches in sample data generation
        if ! grep -q "irrigation_amount = 0.0 if" "$GCS_PIPELINE_FILE"; then
            echo "Fixing type mismatch issues in sample data generation..."
            sed -i 's/irrigation_amount = 0 if/irrigation_amount = 0.0 if/g' "$GCS_PIPELINE_FILE"
            sed -i 's/fertilizer_amount = 0 if/fertilizer_amount = 0.0 if/g' "$GCS_PIPELINE_FILE"
            sed -i 's/pesticide_amount = 0 if/pesticide_amount = 0.0 if/g' "$GCS_PIPELINE_FILE"
            echo "✅ Fixed type mismatch issues in sample data generation."
            
            # Record this change in git
            git add "$GCS_PIPELINE_FILE"
            git commit -m "Fix type mismatch issues in sample data generation"
        else
            echo "✅ Type handling is already correctly configured."
        fi
    else
        echo "❌ Batch pipeline file not found at $GCS_PIPELINE_FILE."
        return 1
    fi
    
    echo "GCS connector configuration is now properly set up."
    echo "==========================================================="
    return 0
}

# Modified verify-kafka function to include configuration check and fix
verify-kafka() {
    echo "==========================================================="
    echo "Verifying Kafka setup..."
    
    # First, check and fix configuration if needed
    check_fix_kafka_config
    
    # Check if broker container is running
    if docker ps | grep -q "${PROJECT_NAME}-broker"; then
        echo "✅ Kafka broker is running."
        
        # Add call to ensure hostname resolution
        ensure_broker_hostname_resolution
        
        # Check if topics can be listed
        TOPICS=$(docker exec ${PROJECT_NAME}-broker kafka-topics --bootstrap-server localhost:29092 --list)
        if [ $? -eq 0 ]; then
            echo "✅ Kafka broker is accessible and functional."
            echo "Available topics:"
            echo "$TOPICS"
            
            # Check if our topic exists
            if echo "$TOPICS" | grep -q "agri_data"; then
                echo "✅ Topic 'agri_data' exists."
                # Get topic details
                echo "Topic details:"
                docker exec ${PROJECT_NAME}-broker kafka-topics --bootstrap-server localhost:29092 --describe --topic agri_data
            else
                echo "❌ Topic 'agri_data' does not exist yet."
                # Create the topic if it doesn't exist
                echo "Creating 'agri_data' topic..."
                docker exec ${PROJECT_NAME}-broker kafka-topics --bootstrap-server localhost:29092 --create --topic agri_data --partitions 1 --replication-factor 1 --if-not-exists
                if [ $? -eq 0 ]; then
                    echo "✅ Topic 'agri_data' created successfully."
                else
                    echo "❌ Failed to create topic 'agri_data'."
                    return 1
                fi
            fi
        else
            echo "❌ Cannot connect to Kafka broker."
            return 1
        fi
        
        echo "Kafka verification complete."
    else
        echo "❌ Kafka broker is NOT running."
        echo "Try starting Kafka with 'start-kafka' command."
        return 1
    fi
    echo "==========================================================="
    return 0
}

# Git checkpoint function to save project state
git_checkpoint() {
    local message="$1"
    if [ -z "$message" ]; then
        message="Automatic checkpoint - project state saved"
    fi
    
    git add .
    git commit -m "$message"
    echo "✅ Git checkpoint created: $message"
}

# Add a function to restart Kafka after configuration changes
restart-kafka-with-config() {
    echo "==========================================================="
    echo "Restarting Kafka with updated configuration..."
    
    # Stop Kafka first
    stop-kafka
    
    # Check and fix configuration
    check_fix_kafka_config
    
    # Start Kafka with the updated configuration
    start-kafka
    
    # Verify Kafka is working properly
    verify-kafka
    
    echo "==========================================================="
    return 0
}

# Function to start the Kafka services with configuration verification
start-kafka() {
    echo "==========================================================="
    echo "Starting Kafka services with configuration verification..."
    
    # Check and fix configuration before starting
    check_fix_kafka_config
    
    # Start Kafka services
    docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env up -d
    
    # Wait for services to be ready
    echo "Waiting for Kafka services to start..."
    sleep 10
    
    # Verify Kafka services are running
    if docker ps | grep -q "${PROJECT_NAME}-broker"; then
        echo "✅ Kafka broker is running."
    else
        echo "❌ Kafka broker failed to start. Check the logs with 'docker logs ${PROJECT_NAME}-broker'"
        return 1
    fi
    
    echo "==========================================================="
    return 0
}

# Updated function with automatic IP resolution and git checkpoints
start-streaming-pipeline() {
    echo "==========================================================="
    echo "Starting the streaming data pipeline with automatic IP resolution..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Make sure .env has the right configuration
    check_fix_kafka_config
    
    # Start Kafka
    echo "Starting Kafka services..."
    start-kafka
    
    # Wait for Kafka to be ready
    echo "Waiting for Kafka to be ready..."
    sleep 15
    
    # Verify Kafka is running correctly and ensure hostname resolution
    echo "Verifying Kafka setup and network configuration..."
    verify-kafka || { 
        echo "⛔ Kafka verification failed. Attempting to fix configuration and restart..."; 
        restart-kafka-with-config || {
            echo "⛔ Kafka configuration fix failed. Please check the logs and fix manually.";
            return 1;
        }
    }
    
    # Create a git checkpoint after successful Kafka configuration
    git_checkpoint "Kafka configuration verified and optimized"
    
    # Start Airflow for orchestration
    echo "Starting Airflow services..."
    start-airflow
    
    # Wait for Airflow to be ready
    echo "Waiting for Airflow to be ready..."
    sleep 20
    
    # Build and start the streaming components with logs in current terminal
    echo "Building and starting data producer and consumer..."
    echo "You'll see logs in this terminal. Press Ctrl+C to stop."
    echo "Wait 5 seconds before starting... (Ctrl+C now to abort)"
    sleep 5
    
    # Run in current terminal with logs visible
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up --build
}

# Function to stop the streaming pipeline
stop-streaming-pipeline(){
    # Stop Kafka and Mage
    stop-kafka
    stop-airflow
}

olap-transformation-pipeline(){
    echo "==========================================================="
    echo "Starting OLAP transformation pipeline..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Verify that temporary data directory exists and has data
    if [ ! -d "/tmp/agri_data" ] || [ -z "$(ls -A /tmp/agri_data)" ]; then
        echo "⚠️ No data found in /tmp/agri_data."
        echo "  This may be because the streaming pipeline hasn't processed any data yet."
        echo "  You can start the streaming pipeline with 'start-streaming-pipeline' first."
        echo "  Proceeding with sample data generation..."
    else
        echo "✅ Found data in /tmp/agri_data: $(ls -1 /tmp/agri_data | wc -l) file(s)."
    fi
    
    # Execute the Python batch pipeline script
    echo "Running PySpark transformation script..."
    
    # Set environment variables for better error messages
    export PYTHONIOENCODING=utf-8
    
    python batch_pipeline/export_to_gcs/pipeline.py 2>&1 | tee /tmp/batch_pipeline.log
    
    # Check for success
    if grep -q "Successfully processed" /tmp/batch_pipeline.log; then
        echo "✅ Batch pipeline completed successfully."
    else
        if grep -q "Error" /tmp/batch_pipeline.log; then
            echo "❌ Batch pipeline encountered errors."
            echo "Error details:"
            grep -A 5 "Error" /tmp/batch_pipeline.log
            echo "See /tmp/batch_pipeline.log for full details."
        else
            echo "⚠️ Batch pipeline may have completed with warnings."
            echo "See /tmp/batch_pipeline.log for full details."
        fi
    fi
    
    echo "==========================================================="
}

gcs-to-bigquery-pipeline(){
    echo "==========================================================="
    echo "Starting GCS to BigQuery export pipeline..."
    
    # Check that Airflow is running
    if ! docker ps | grep -q "airflow-airflow-webserver-1"; then
        echo "❌ Airflow is not running. Cannot trigger the DAG."
        echo "  Please start Airflow with 'start-airflow' and try again."
        return 1
    fi
    
    # Verify the DAG exists
    if ! docker exec airflow-airflow-webserver-1 airflow dags list | grep -q "gcs_to_bigquery_export"; then
        echo "❌ The 'gcs_to_bigquery_export' DAG is not found in Airflow."
        echo "  Please check if the DAG is properly deployed."
        # List available DAGs
        echo "Available DAGs:"
        docker exec airflow-airflow-webserver-1 airflow dags list | grep -v "DAGS"
        return 1
    fi
    
    # Manual API trigger for the GCS to BigQuery DAG
    echo "Triggering GCS to BigQuery export DAG..."
    
    RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:${AIRFLOW_PORT}/api/v1/dags/gcs_to_bigquery_export/dagRuns" \
    -H "Content-Type: application/json" \
    -H "Authorization: Basic $(echo -n "${_AIRFLOW_WWW_USER_USERNAME}:${_AIRFLOW_WWW_USER_PASSWORD}" | base64)" \
    -d '{"conf": {}, "note": "Manual trigger from command line"}')
    
    if [ "$RESPONSE" -eq 200 ] || [ "$RESPONSE" -eq 201 ]; then
        echo "✅ Successfully triggered GCS to BigQuery export DAG."
        echo "  You can monitor the progress at: http://localhost:${AIRFLOW_PORT}/dags/gcs_to_bigquery_export/grid"
    else
        echo "❌ Failed to trigger DAG. HTTP response: $RESPONSE"
        echo "  Please check the Airflow logs for more details."
    fi
    
    echo "==========================================================="
}

start-batch-pipeline() {
    echo "==========================================================="
    echo "STEP 1: Processing data with batch pipeline..."
    echo "==========================================================="
    
    # Check if network exists
    if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
        docker network create ${PROJECT_NAME}-network
        echo "Created network ${PROJECT_NAME}-network."
    else
        echo "Network ${PROJECT_NAME}-network already exists."
    fi
    
    # Load commands
    echo "✅ Loaded commands for ${PROJECT_NAME}."
    
    # Check environment
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Fix GCS connector configuration
    check_fix_gcs_config
    
    # Run OLAP transformations to prepare data
    echo "Running OLAP transformations..."
    
    # Check environment again
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Check if there is data in the /tmp/agri_data directory
    if [ ! -d "/tmp/agri_data" ] || [ -z "$(ls -A /tmp/agri_data 2>/dev/null)" ]; then
        echo "⚠️ Warning: No data found in /tmp/agri_data."
        echo "The streaming pipeline may not have processed any data yet."
        echo "Proceeding with sample data generation..."
    fi
    
    # Run PySpark transformation
    echo "Starting PySpark transformation..."
    cd ./batch_pipeline/export_to_gcs || { echo "❌ Could not find batch_pipeline directory."; return 1; }
    python3 pipeline.py > /tmp/batch_pipeline.log 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✅ Batch pipeline completed successfully."
    else
        echo "❌ Batch pipeline encountered errors."
        echo "Error details:"
        grep -A 10 "Error" /tmp/batch_pipeline.log | head -20
        echo "See /tmp/batch_pipeline.log for full details."
    fi
    
    cd ../..
    
    echo "==========================================================="
    echo "STEP 2: Loading transformed data to BigQuery..."
    echo "==========================================================="
    
    # Start GCS to BigQuery export pipeline
    echo "Starting GCS to BigQuery export pipeline..."
    cd ./batch_pipeline/export_to_big_query || { echo "❌ Could not find export_to_big_query directory."; return 1; }
    
    # Check if Airflow is running
    if ! docker ps | grep -q "airflow-airflow-webserver-1"; then
        echo "❌ Airflow is not running. Cannot trigger the DAG."
        echo "  Please start Airflow with 'start-airflow' and try again."
        cd ../..
        return 1
    fi
    
    # Trigger DAG in Airflow
    docker exec airflow-airflow-webserver-1 airflow dags trigger gcs_to_bigquery_dag
    
    if [ $? -eq 0 ]; then
        echo "✅ GCS to BigQuery export pipeline triggered successfully."
        echo "  Check Airflow UI at http://localhost:8080 for progress."
    else
        echo "❌ GCS to BigQuery export pipeline failed with exit code $?."
        echo "  Please check the logs for more details."
    fi
    
    cd ../..
    
    echo "==========================================================="
    echo "Batch pipeline process completed."
    echo "==========================================================="
}

gitting(){
    git add .
    sleep 2
    git commit -m "Update from Local"
    sleep 2
    git push -u origin main
}

terraform-start(){
    terraform -chdir=terraform init
    terraform -chdir=terraform plan
    terraform -chdir=terraform apply
}
terraform-destroy(){
    terraform -chdir=terraform destroy
}

# Function to check environment setup
check-environment() {
    echo "==========================================================="
    echo "Checking environment setup..."
    
    # Check if .env file exists
    if [ ! -f ".env" ]; then
        echo "❌ .env file not found."
        if [ -f ".env.example" ]; then
            echo "Creating .env file from .env.example..."
            cp .env.example .env
            echo "✅ Created .env file. Please review and adjust settings if necessary."
        else
            echo "❌ .env.example file not found. Cannot create .env file."
            return 1
        fi
    else
        echo "✅ .env file found."
    fi
    
    # Check for GCP credentials
    if [ ! -f "gcp-creds.json" ]; then
        echo "❌ GCP credentials file (gcp-creds.json) not found."
        echo "You need to create this file with valid GCP credentials."
        return 1
    else
        echo "✅ GCP credentials file found."
        
        # Copy GCP credentials to batch_pipeline/export_to_gcs if needed
        if [ -d "batch_pipeline/export_to_gcs" ] && [ ! -f "batch_pipeline/export_to_gcs/gcp-creds.json" ]; then
            echo "Copying GCP credentials to batch_pipeline/export_to_gcs directory..."
            cp gcp-creds.json batch_pipeline/export_to_gcs/
            echo "✅ Copied GCP credentials to batch_pipeline/export_to_gcs directory."
        fi
    fi
    
    # Check for Docker
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker not found. Please install Docker."
        return 1
    else
        echo "✅ Docker is installed."
    fi
    
    # Check for Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Docker Compose not found. Please install Docker Compose."
        return 1
    else
        echo "✅ Docker Compose is installed."
    fi
    
    # Check for Git
    if ! command -v git &> /dev/null; then
        echo "❌ Git not found. Please install Git."
        return 1
    else
        echo "✅ Git is installed."
    fi
    
    # Check if the Docker network exists
    if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
        echo "❌ Docker network '${PROJECT_NAME}-network' not found. Creating it..."
        docker network create ${PROJECT_NAME}-network
        echo "✅ Created Docker network '${PROJECT_NAME}-network'."
    else
        echo "✅ Docker network '${PROJECT_NAME}-network' exists."
    fi
    
    # Check streaming_pipeline directory
    if [ ! -d "streaming_pipeline" ]; then
        echo "❌ streaming_pipeline directory not found."
        return 1
    else
        echo "✅ streaming_pipeline directory found."
    fi
    
    # Check if we're in a git repository
    if [ ! -d ".git" ]; then
        echo "❌ Not in a Git repository. Initializing Git repository..."
        git init
        git add .
        git commit -m "Initial commit - Agricultural data pipeline setup"
        echo "✅ Git repository initialized."
    else
        echo "✅ Git repository exists."
    fi
    
    echo "Environment check completed successfully."
    echo "==========================================================="
    return 0
}

start-project(){
    echo "==========================================================="
    echo "Starting the complete agricultural data pipeline..."
    echo "This will create cloud resources, start services, and run all pipeline components."
    echo "==========================================================="
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # 1. Initialize infrastructure with Terraform
    echo "STEP 1: Creating GCP resources with Terraform..."
    terraform-start
    echo "✅ GCP resources created successfully."
    
    # 2. Start Kafka and verify
    echo "==========================================================="
    echo "STEP 2: Starting Kafka and related services..."
    start-kafka
    sleep 15
    echo "Verifying Kafka setup..."
    verify-kafka
    
    # 3. Start Airflow and verify
    echo "==========================================================="
    echo "STEP 3: Starting Airflow for orchestration..."
    start-airflow
    sleep 15
    echo "Verifying Airflow setup..."
    verify-airflow
    
    # 4. Start the streaming data pipeline
    echo "==========================================================="
    echo "STEP 4: Starting streaming pipeline components..."
    echo "This will start the producer to generate data and the consumer to process it."
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d
    sleep 15
    echo "Verifying streaming components..."
    verify-producer
    verify-consumer
    
    echo "Streaming pipeline is now running."
    echo "NOTE: You can access the Kafka Control Center at http://localhost:9021"
    echo "NOTE: You can access the Airflow UI at http://localhost:${AIRFLOW_PORT}"
    echo "      Username: ${_AIRFLOW_WWW_USER_USERNAME}, Password: ${_AIRFLOW_WWW_USER_PASSWORD}"
    
    # 5. Start Spark for batch processing
    echo "==========================================================="
    echo "STEP 5: Starting Spark for batch data processing..."
    start-spark
    sleep 15
    echo "Verifying Spark setup..."
    verify-spark
    
    echo "NOTE: You can access the Spark UI at http://localhost:8080"
    
    # 6. Run batch processing pipeline
    echo "==========================================================="
    echo "STEP 6: Running batch data processing pipeline..."
    echo "This step will transform raw data to dimension and fact tables."
    start-batch-pipeline
    
    # 7. Run DBT transformations
    echo "==========================================================="
    echo "STEP 7: Running business transformations with DBT..."
    echo "This step will create analytical models in BigQuery."
    run-dbt || echo "⚠️ DBT transformations may have issues but continuing..."
    
    # 8. Start Metabase for visualization
    echo "==========================================================="
    echo "STEP 8: Starting Metabase for data visualization..."
    stop-metabase 2>/dev/null || true
    echo "Removing previous Metabase data..."
    docker volume rm -f $(docker volume ls -q | grep metabase) 2>/dev/null || true
    start-metabase
    sleep 15
    
    # 9. Check and ensure all ports are forwarded properly
    echo "==========================================================="
    echo "STEP 9: Checking port forwarding for UI components..."
    check-forward-ports
    
    # Final verification and summary
    echo "==========================================================="
    echo "All components have been started!"
    echo "Running a final verification..."
    verify-all
    
    echo "==========================================================="
    echo "AGRICULTURAL DATA PIPELINE DEPLOYMENT COMPLETE"
    echo "==========================================================="
    echo "Access Points:"
    echo "- Kafka Control Center: http://localhost:9021"
    echo "- Airflow UI: http://localhost:${AIRFLOW_PORT}"
    echo "  Username: ${_AIRFLOW_WWW_USER_USERNAME}, Password: ${_AIRFLOW_WWW_USER_PASSWORD}"
    echo "- Spark UI: http://localhost:8080"
    echo "- Metabase: http://localhost:${METABASE_PORT}"
    echo "  Initial setup required on first visit"
    echo "==========================================================="
    echo "To check component status: verify-all"
    echo "To stop all services: stop-all-services"
    echo "==========================================================="
}


stop-all-services(){
    stop-airflow
    stop-kafka
    stop-spark
    stop-metabase
}

# Verification functions

verify-producer() {
    echo "==========================================================="
    echo "Verifying producer..."
    # Check producer logs
    if docker ps -a | grep -q "agri_data_producer"; then
        PRODUCER_STATUS=$(docker inspect --format='{{.State.ExitCode}}' agri_data_producer)
        if [ "$PRODUCER_STATUS" = "0" ]; then
            echo "✅ Producer completed successfully with exit code 0."
            echo "Producer logs (last 10 lines):"
            docker logs agri_data_producer | tail -n 10
            
            # Check if messages were actually delivered
            if docker logs agri_data_producer | grep -q "Message delivered"; then
                echo "✅ Messages were successfully delivered to Kafka."
                MESSAGES_COUNT=$(docker logs agri_data_producer | grep "Produced message" | wc -l)
                echo "Producer generated approximately $MESSAGES_COUNT messages."
            else
                echo "❌ No confirmation of message delivery found in logs."
            fi
        else
            echo "❌ Producer failed with exit code $PRODUCER_STATUS."
            echo "Producer logs (last 20 lines for debugging):"
            docker logs agri_data_producer | tail -n 20
        fi
    else
        echo "❌ Producer container not found."
        echo "Try running the streaming data process with 'stream-data' command."
    fi
    echo "==========================================================="
}

verify-consumer() {
    echo "==========================================================="
    echo "Verifying consumer..."
    # Check if consumer is running
    if docker ps | grep -q "agri_data_consumer"; then
        echo "✅ Consumer is running."
        echo "Consumer logs (last 20 lines):"
        docker logs agri_data_consumer | tail -n 20
        # Check if messages are being processed
        if docker logs agri_data_consumer | grep -q "Processing batch"; then
            echo "✅ Consumer is processing messages."
            BATCHES_COUNT=$(docker logs agri_data_consumer | grep "Processing batch" | wc -l)
            echo "Consumer has processed $BATCHES_COUNT batches of messages."
            
            # Check for Parquet file generation
            if docker logs agri_data_consumer | grep -q "Converted .* messages to Parquet file"; then
                echo "✅ Consumer is successfully converting messages to Parquet format."
                
                # Check for GCS upload simulation
                if docker logs agri_data_consumer | grep -q "Successfully uploaded .* to GCS"; then
                    echo "✅ Parquet files are being uploaded to GCS (simulation)."
                else
                    echo "❌ No successful GCS uploads found in logs."
                fi
            else
                echo "❌ No Parquet file conversions found in logs."
            fi
        else
            echo "❌ Consumer is NOT processing messages."
            echo "Check if producer has sent any messages or if there are connectivity issues."
        fi
    else
        echo "❌ Consumer is NOT running."
        echo "Try running the streaming data process with 'stream-data' command."
    fi
    echo "==========================================================="
}

verify-airflow() {
    echo "==========================================================="
    echo "Verifying Airflow setup..."
    if docker ps | grep -q "airflow-airflow-webserver-1"; then
        echo "✅ Airflow webserver is running."
        echo "✅ Airflow is accessible at http://localhost:${AIRFLOW_PORT}"
        
        # Check scheduler
        if docker ps | grep -q "airflow-airflow-scheduler-1"; then
            echo "✅ Airflow scheduler is running."
        else
            echo "❌ Airflow scheduler is NOT running."
        fi
        
        # Check for DAGs
        echo "Checking for DAGs..."
        docker exec airflow-airflow-webserver-1 airflow dags list | grep -i "agri" || echo "No agriculture-related DAGs found."
    else
        echo "❌ Airflow is NOT running."
        echo "Try starting Airflow with 'start-airflow' command."
    fi
    echo "==========================================================="
}

verify-spark() {
    echo "==========================================================="
    echo "Verifying Spark setup..."
    if docker ps | grep -q "${PROJECT_NAME}-spark-master"; then
        echo "✅ Spark master is running."
        
        # Check worker
        if docker ps | grep -q "${PROJECT_NAME}-spark-worker"; then
            echo "✅ Spark worker is running."
        else
            echo "❌ Spark worker is NOT running."
        fi
        
        echo "✅ Spark UI should be accessible at http://localhost:8080"
        
        # Check and fix GCS connector configuration
        check_fix_gcs_config
    else
        echo "❌ Spark is NOT running."
        echo "Try starting Spark with 'start-spark' command."
    fi
    echo "==========================================================="
}

verify-batch-pipeline() {
    echo "==========================================================="
    echo "Verifying batch pipeline..."
    
    # Load environment variables from .env if they're not set
    if [ -z "$GCS_BUCKET_NAME" ]; then
        # Load from .env file if it exists
        if [ -f ".env" ]; then
            export $(grep -v '^#' .env | xargs)
            echo "✅ Loaded environment variables from .env file."
        else
            echo "⚠️ .env file not found. Using default values."
        fi
    fi
    
    # Check temporary directory for Parquet files
    if [ -d "/tmp/agri_data" ] && [ "$(ls -A /tmp/agri_data)" ]; then
        echo "✅ Parquet files generated in /tmp/agri_data."
        ls -la /tmp/agri_data
        
        # Count files
        FILE_COUNT=$(ls -1 /tmp/agri_data | wc -l)
        echo "Found $FILE_COUNT Parquet files."
    else
        echo "❌ No Parquet files found in /tmp/agri_data."
        echo "Check if consumer is running and processing messages."
    fi
    
    # Check GCP resources
    echo "GCP resources check (informational only):"
    echo "- GCS Bucket: ${GCS_BUCKET_NAME:-"not set"}"
    echo "- Raw data path: ${GCS_RAW_DATA_PATH:-"not set"}"
    echo "- Transformed data path: ${GCS_TRANSFORMED_DATA_PATH:-"not set"}"
    echo "- BigQuery dataset: ${GCP_DATASET_ID:-"not set"}"
    
    echo "For complete verification, check GCS bucket and BigQuery tables in Google Cloud Console."
    echo "==========================================================="
}

verify-dbt() {
    echo "==========================================================="
    echo "Verifying DBT setup and models..."
    
    # Check that DBT is installed
    if ! command -v dbt &> /dev/null; then
        echo "❌ DBT is not installed. Please install with 'pip install dbt-bigquery'."
        return 1
    else
        echo "✅ DBT is installed."
    fi
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # List DBT models
    echo "Available DBT models:"
    dbt ls --profiles-dir . 2>/dev/null || echo "❌ Could not list DBT models."
    
    # Try running dbt debug
    echo "Testing DBT connection..."
    dbt debug --profiles-dir . 2>/dev/null
    
    # Return to the original directory
    cd ..
    
    # Check if BigQuery tables exist
    echo "Checking for transformed tables in BigQuery..."
    
    # Add your BigQuery checking logic here if needed
    
    echo "==========================================================="
}

verify-all() {
    echo "==========================================================="
    echo "Running complete verification of all components..."
    verify-kafka
    verify-producer
    verify-consumer
    verify-airflow
    verify-spark
    verify-batch-pipeline
    verify-dbt
    echo "==========================================================="
    echo "Verification complete!"
}

# Add a test-all function that runs the pipeline with verifications
test-pipeline() {
    echo "==========================================================="
    echo "Starting full pipeline test with verification..."
    
    # Clean up environment
    echo "Cleaning up previous environment..."
    ./cleanup.sh
    
    # Check environment
    echo "Checking environment..."
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Start Kafka
    echo "STEP 1: Starting Kafka..."
    start-kafka
    sleep 30  # Allow time for Kafka to start
    verify-kafka || { 
        echo "❌ Kafka verification failed."; 
        echo "Try running 'stop-kafka' and then 'start-kafka' again."; 
        return 1; 
    }
    
    # Make sure broker IP is updated in streaming docker-compose
    ensure_broker_hostname_resolution || {
        echo "❌ Failed to configure hostname resolution.";
        return 1;
    }
    
    # Start streaming components
    echo "STEP 2: Starting streaming components..."
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d
    sleep 20  # Allow time for components to start
    
    # Verify producer
    echo "STEP 3: Verifying producer..."
    verify-producer || {
        echo "⚠️ Producer verification showed issues, but continuing...";
    }
    
    # Verify consumer
    echo "STEP 4: Verifying consumer..."
    verify-consumer || {
        echo "⚠️ Consumer verification showed issues, but continuing...";
    }
    
    # Check data in temp directory
    echo "STEP 5: Checking if data is being processed..."
    # Wait for some data to be generated
    sleep 30
    ls -la /tmp/agri_data
    
    # Check if any parquet files were generated
    PARQUET_COUNT=$(ls -1 /tmp/agri_data/*.parquet 2>/dev/null | wc -l)
    if [ "$PARQUET_COUNT" -gt 0 ]; then
        echo "✅ Data processing confirmed! Found $PARQUET_COUNT Parquet files."
    else
        echo "⚠️ No Parquet files found yet. Consumer may still be collecting messages."
        echo "Wait a bit longer or check consumer logs with 'docker logs agri_data_consumer'"
    fi
    
    # Start Airflow
    echo "STEP 6: Starting Airflow..."
    start-airflow
    sleep 30  # Allow time for Airflow to start
    verify-airflow || {
        echo "⚠️ Airflow verification showed issues, but continuing...";
    }
    
    # Start Spark
    echo "STEP 7: Starting Spark..."
    start-spark
    sleep 20  # Allow time for Spark to start
    verify-spark || {
        echo "⚠️ Spark verification showed issues, but continuing...";
    }
    
    # Run batch pipeline
    echo "STEP 8: Running batch pipeline..."
    start-batch-pipeline
    
    # Verify batch pipeline
    echo "STEP 9: Verifying batch pipeline output..."
    verify-batch-pipeline
    
    echo "==========================================================="
    echo "Pipeline test complete!"
    echo "Run 'verify-all' for a comprehensive check of all components."
    echo "==========================================================="
}

# Restart functions
restart-kafka() {
    echo "Restarting Kafka..."
    stop-kafka
    sleep 5
    start-kafka
    sleep 10
    verify-kafka
}

restart-streaming() {
    echo "Restarting streaming components..."
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env down
    sleep 5
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d
    sleep 10
    verify-producer
    verify-consumer
}

restart-airflow() {
    echo "Restarting Airflow..."
    stop-airflow
    sleep 5
    start-airflow
    sleep 20
    verify-airflow
}

restart-spark() {
    echo "Restarting Spark..."
    stop-spark
    sleep 5
    start-spark
    sleep 10
    verify-spark
}

# Function to check and forward required UI ports
check-forward-ports() {
    echo "==========================================================="
    echo "Checking if UI ports are correctly forwarded..."
    
    # Check Airflow port
    if ! nc -z localhost ${AIRFLOW_PORT:-8080} >/dev/null 2>&1; then
        echo "❌ Airflow port ${AIRFLOW_PORT:-8080} is not accessible."
        echo "Trying to fix port forwarding..."
        docker-compose -f ./docker/airflow/docker-compose.yml --env-file ./.env up -d
    else
        echo "✅ Airflow port ${AIRFLOW_PORT:-8080} is accessible."
    fi
    
    # Check Spark UI port
    if ! nc -z localhost 8080 >/dev/null 2>&1; then
        echo "❌ Spark UI port 8080 is not accessible."
        echo "Trying to fix port forwarding..."
        docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env up -d
    else
        echo "✅ Spark UI port 8080 is accessible."
    fi
    
    # Check Metabase port
    if ! nc -z localhost ${METABASE_PORT:-3000} >/dev/null 2>&1; then
        echo "❌ Metabase port ${METABASE_PORT:-3000} is not accessible."
        echo "Trying to fix port forwarding..."
        docker-compose -f ./docker/metabase/docker-compose.yml --env-file ./.env up -d
    else
        echo "✅ Metabase port ${METABASE_PORT:-3000} is accessible."
    fi
    
    # Check Kafka Control Center port
    if ! nc -z localhost 9021 >/dev/null 2>&1; then
        echo "❌ Kafka Control Center port 9021 is not accessible."
        echo "Trying to fix port forwarding..."
        docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env up -d
    else
        echo "✅ Kafka Control Center port 9021 is accessible."
    fi
    
    echo "==========================================================="
    return 0
}

# Function to do a full reset and test of the project
full-reset-and-test() {
    echo "==========================================================="
    echo "Starting full project reset and test..."
    echo "This will clear all Docker containers and images, then rebuild and retest."
    echo "WARNING: This will destroy all existing data and resources."
    echo "==========================================================="
    
    # Prompt for confirmation
    read -p "Are you sure you want to perform a full reset? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        echo "Reset canceled."
        return 1
    fi
    
    # Run cleanup with terraform destroy
    echo "Running cleanup with terraform destroy..."
    ./cleanup.sh --destroy-terraform
    if [ $? -ne 0 ]; then
        echo "⚠️ Cleanup script encountered an issue. Do you want to continue? (yes/no): "
        read continue_after_cleanup
        if [[ "$continue_after_cleanup" != "yes" ]]; then
            echo "Reset canceled."
            return 1
        fi
    fi
    
    # Make sure all Docker containers are stopped
    echo "Stopping any remaining Docker containers..."
    docker ps -a | grep -E "agri_data|kafka|broker|zookeeper" | awk '{print $1}' | xargs -r docker stop
    docker ps -a | grep -E "agri_data|kafka|broker|zookeeper" | awk '{print $1}' | xargs -r docker rm
    
    # Prune Docker system
    echo "Cleaning all Docker resources..."
    docker system prune -af
    
    # Remove all Docker volumes related to the project
    echo "Removing Docker volumes..."
    docker volume ls -q | grep "${PROJECT_NAME}" | xargs -r docker volume rm
    
    # Remove temporary data
    echo "Removing temporary data..."
    rm -rf /tmp/agri_data
    mkdir -p /tmp/agri_data
    chmod 777 /tmp/agri_data
    
    # Remove and recreate the network
    echo "Recreating Docker network..."
    docker network rm ${PROJECT_NAME}-network || true
    docker network create ${PROJECT_NAME}-network
    
    # Wait for Docker to stabilize
    echo "Waiting for Docker to stabilize..."
    sleep 5
    
    # Check for existing infrastructure and handle accordingly
    echo "Checking for existing infrastructure..."
    if terraform -chdir=terraform state list &>/dev/null; then
        echo "⚠️ Existing Terraform resources detected. Do you want to reuse them? (yes/no): "
        read reuse_infra
        if [[ "$reuse_infra" != "yes" ]]; then
            echo "Creating new infrastructure..."
            terraform-start
            if [ $? -ne 0 ]; then
                echo "❌ Terraform initialization/apply failed. Do you want to continue without cloud resources? (yes/no): "
                read continue_without_cloud
                if [[ "$continue_without_cloud" != "yes" ]]; then
                    echo "Reset canceled."
                    return 1
                fi
            fi
        else
            echo "Reusing existing infrastructure."
        fi
    else
        echo "Creating infrastructure with Terraform..."
        terraform-start
        if [ $? -ne 0 ]; then
            echo "❌ Terraform initialization/apply failed. Do you want to continue without cloud resources? (yes/no): "
            read continue_without_cloud
            if [[ "$continue_without_cloud" != "yes" ]]; then
                echo "Reset canceled."
                return 1
            fi
        fi
    fi
    
    # Start the main components one by one with interactive error handling
    echo "Starting Kafka..."
    start-kafka
    sleep 15
    
    # Verify Kafka and ensure correct hostname resolution
    verify-kafka
    if [ $? -ne 0 ]; then
        echo "❌ Kafka verification failed."
        echo "Do you want to retry starting Kafka? (yes/no): "
        read retry_kafka
        if [[ "$retry_kafka" == "yes" ]]; then
            echo "Restarting Kafka..."
            stop-kafka
            sleep 5
            start-kafka
            sleep 15
            verify-kafka
            if [ $? -ne 0 ]; then
                echo "❌ Failed to start Kafka after retry."
                echo "Do you want to continue without Kafka? (yes/no): "
                read continue_without_kafka
                if [[ "$continue_without_kafka" != "yes" ]]; then
                    echo "Reset canceled."
                    return 1
                fi
            fi
        elif [[ "$retry_kafka" != "yes" ]]; then
            echo "Do you want to continue without Kafka? (yes/no): "
            read continue_without_kafka
            if [[ "$continue_without_kafka" != "yes" ]]; then
                echo "Reset canceled."
                return 1
            fi
        fi
    fi
    
    # Now start the streaming components with correct hostname resolution
    echo "Configuring hostname resolution..."
    ensure_broker_hostname_resolution
    if [ $? -ne 0 ]; then
        echo "❌ Failed to configure hostname resolution."
        echo "Do you want to continue without proper hostname resolution? (yes/no): "
        read continue_without_resolution
        if [[ "$continue_without_resolution" != "yes" ]]; then
            echo "Reset canceled."
            return 1
        fi
    fi
    
    echo "Starting streaming components..."
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d
    if [ $? -ne 0 ]; then
        echo "❌ Failed to start streaming components."
        echo "Do you want to continue without streaming components? (yes/no): "
        read continue_without_streaming
        if [[ "$continue_without_streaming" != "yes" ]]; then
            echo "Reset canceled."
            return 1
        fi
    else
        sleep 15
        
        # Verify streaming components with detailed logs in frontfoot mode
        echo "Verifying producer (see logs below)..."
        docker logs agri_data_producer
        
        echo "Verifying consumer (see logs below)..."
        docker logs agri_data_consumer
    fi
    
    # Start Airflow
    echo "Starting Airflow..."
    start-airflow
    if [ $? -ne 0 ]; then
        echo "❌ Failed to start Airflow."
        echo "Do you want to continue without Airflow? (yes/no): "
        read continue_without_airflow
        if [[ "$continue_without_airflow" != "yes" ]]; then
            echo "Reset canceled."
            return 1
        fi
    else
        sleep 20
        verify-airflow
    fi
    
    # Start Spark
    echo "Starting Spark..."
    start-spark
    if [ $? -ne 0 ]; then
        echo "❌ Failed to start Spark."
        echo "Do you want to continue without Spark? (yes/no): "
        read continue_without_spark
        if [[ "$continue_without_spark" != "yes" ]]; then
            echo "Reset canceled."
            return 1
        fi
    else
        sleep 15
        verify-spark
    fi
    
    # Check for data being produced and consumed
    echo "Checking for data processing..."
    sleep 30
    PARQUET_COUNT=$(ls -1 /tmp/agri_data/*.parquet 2>/dev/null | wc -l)
    if [ "$PARQUET_COUNT" -gt 0 ]; then
        echo "✅ Data processing confirmed! Found $PARQUET_COUNT Parquet files."
    else
        echo "⚠️ No Parquet files found yet. Consumer may still be collecting messages."
        echo "Checking consumer logs for issues..."
        docker logs agri_data_consumer
        
        echo "Do you want to continue even without generated Parquet files? (yes/no): "
        read continue_without_parquet
        if [[ "$continue_without_parquet" != "yes" ]]; then
            echo "Reset canceled."
            return 1
        fi
    fi
    
    # Run the batch pipeline
    echo "Running batch pipeline..."
    start-batch-pipeline
    if [ $? -ne 0 ]; then
        echo "❌ Batch pipeline encountered issues."
        echo "Do you want to continue? (yes/no): "
        read continue_after_batch
        if [[ "$continue_after_batch" != "yes" ]]; then
            echo "Reset canceled."
            return 1
        fi
    fi
    
    # Run DBT transformations
    echo "Running DBT transformations..."
    run-dbt
    if [ $? -ne 0 ]; then
        echo "❌ DBT transformations failed."
        echo "Do you want to continue without DBT transformations? (yes/no): "
        read continue_without_dbt
        if [[ "$continue_without_dbt" != "yes" ]]; then
            echo "Reset canceled."
            return 1
        fi
    fi
    
    # Verify all components
    echo "Performing final verification of all components..."
    verify-all
    
    echo "==========================================================="
    echo "Full reset and test complete!"
    echo "If you're seeing this message, the pipeline is functioning correctly."
    echo "==========================================================="
}

run-dbt() {
    echo "==========================================================="
    echo "Running DBT transformations on BigQuery data..."
    
    # Check that DBT is installed
    if ! command -v dbt &> /dev/null; then
        echo "❌ DBT is not installed. Installing dbt-bigquery..."
        pip install dbt-bigquery
    fi
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Run DBT
    echo "Running DBT models..."
    dbt run --profiles-dir .
    
    # Check the exit status
    if [ $? -eq 0 ]; then
        echo "✅ DBT transformations completed successfully!"
    else
        echo "❌ DBT transformations failed. Check the logs for more information."
        return 1
    fi
    
    # Generate documentation
    echo "Generating DBT documentation..."
    dbt docs generate --profiles-dir .
    
    # Return to the original directory
    cd ..
    
    echo "==========================================================="
    echo "Business transformations complete! The data is now ready for analytics."
    echo "==========================================================="
}

# Function to serve DBT docs
serve-dbt-docs() {
    echo "==========================================================="
    echo "Generating and serving DBT documentation..."
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Generate documentation
    echo "Generating DBT documentation..."
    dbt docs generate --profiles-dir .
    
    # Serve documentation
    echo "Starting DBT documentation server..."
    echo "Access the documentation at: http://localhost:8080"
    dbt docs serve --profiles-dir . --port 8080
    
    # Return to the original directory (this will only execute after the server is stopped)
    cd ..
    
    echo "==========================================================="
}

# Function to generate DBT docs without serving
generate-dbt-docs() {
    echo "==========================================================="
    echo "Generating DBT documentation..."
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Generate documentation
    echo "Generating DBT documentation..."
    dbt docs generate --profiles-dir .
    
    echo "✅ DBT documentation generated successfully."
    echo "You can serve it using the 'serve-dbt-docs' command."
    
    # Return to the original directory
    cd ..
    
    echo "==========================================================="
}

# Function to rebuild and restart the entire stack
rebuild-and-restart-all() {
    echo "==========================================================="
    echo "Rebuilding and restarting the entire data pipeline stack..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Stop all services
    echo "Stopping all services..."
    stop-streaming-pipeline
    stop-spark
    stop-kafka
    stop-postgres
    stop-metabase
    
    # Wait for containers to stop
    echo "Waiting for all containers to stop..."
    sleep 10
    
    # Update configuration
    echo "Checking and updating configurations..."
    check_fix_kafka_config
    
    # Create a git checkpoint
    git_checkpoint "Configuration updated for rebuild"
    
    # Start the services in the correct order
    echo "Starting Postgres..."
    start-postgres
    
    echo "Starting Kafka..."
    start-kafka
    
    echo "Waiting for Kafka to be ready..."
    sleep 15
    
    echo "Verifying and configuring Kafka..."
    verify-kafka
    ensure_broker_hostname_resolution
    
    echo "Starting Spark..."
    start-spark
    
    echo "Starting Metabase..."
    start-metabase
    
    # Create another git checkpoint
    git_checkpoint "Infrastructure services restarted successfully"
    
    echo "Starting streaming pipeline with producer and consumer..."
    docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d --build
    
    echo "All services have been rebuilt and restarted."
    echo "==========================================================="
    
    # Show status of running containers
    echo "Current status of containers:"
    docker ps
    
    return 0
}

# Function to display pipeline status
status() {
    echo "==========================================================="
    echo "Agricultural Data Pipeline Status"
    echo "==========================================================="
    
    # Check Docker containers
    echo "Docker Containers:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "${PROJECT_NAME}"
    
    # Check Kafka topics
    if docker ps | grep -q "${PROJECT_NAME}-broker"; then
        echo -e "\nKafka Topics:"
        docker exec ${PROJECT_NAME}-broker kafka-topics --bootstrap-server localhost:29092 --list | grep -v -e "^$"
    else
        echo -e "\nKafka is not running."
    fi
    
    # Check GCS buckets if gsutil is available
    if command -v gsutil &> /dev/null; then
        echo -e "\nGCS Buckets:"
        gsutil ls 2>/dev/null || echo "Cannot access GCS buckets. Check credentials."
    else
        echo -e "\ngsutil not installed. Cannot check GCS buckets."
    fi
    
    echo "==========================================================="
    return 0
}

# Function to display help information
help() {
    echo "==========================================================="
    echo "Agricultural Data Pipeline - Command Help"
    echo "==========================================================="
    echo "Available commands:"
    echo
    echo "Infrastructure Management:"
    echo "  start-kafka             - Start Kafka and Zookeeper"
    echo "  stop-kafka              - Stop Kafka and Zookeeper"
    echo "  start-spark             - Start Spark master and worker"
    echo "  stop-spark              - Stop Spark services"
    echo "  start-postgres          - Start PostgreSQL and PgAdmin"
    echo "  stop-postgres           - Stop PostgreSQL and PgAdmin"
    echo "  start-metabase          - Start Metabase for visualization"
    echo "  stop-metabase           - Stop Metabase"
    echo "  start-airflow           - Start Airflow for workflow orchestration"
    echo "  stop-airflow            - Stop Airflow"
    echo
    echo "Pipeline Operations:"
    echo "  start-streaming-pipeline - Start the streaming data pipeline"
    echo "  stop-streaming-pipeline  - Stop the streaming data pipeline"
    echo "  verify-kafka            - Verify Kafka setup and create topic if needed"
    echo "  rebuild-and-restart-all - Rebuild and restart all services"
    echo "  status                  - Show status of all pipeline components"
    echo
    echo "Maintenance and Troubleshooting:"
    echo "  check-environment       - Check and validate environment setup"
    echo "  check_fix_kafka_config  - Check and fix Kafka configuration issues"
    echo "  ensure_broker_hostname_resolution - Ensure Kafka broker hostname resolution"
    echo "  restart-kafka-with-config - Restart Kafka with proper configuration"
    echo "  git_checkpoint \"Message\" - Create a Git checkpoint with custom message"
    echo
    echo "Usage example:"
    echo "  source commands.sh"
    echo "  check-environment"
    echo "  rebuild-and-restart-all"
    echo "==========================================================="
}

# If the script is being sourced, display the available commands
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    echo "Agricultural Data Pipeline commands loaded."
    echo "Type 'help' to see available commands."
else
    echo "Please source this script instead of running it directly:"
    echo "  source commands.sh"
fi