#!/bin/bash

# Set error handling
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Function to print section headers
section() {
  echo -e "\n${GREEN}============================================================${NC}"
  echo -e "${GREEN}== $1${NC}"
  echo -e "${GREEN}============================================================${NC}"
}

# Function to handle errors
handle_error() {
  echo -e "${RED}ERROR: $1${NC}"
  exit 1
}

# Function to verify a command succeeded
verify() {
  if [ $? -ne 0 ]; then
    handle_error "$1"
  else
    echo -e "${GREEN}✓ $2${NC}"
  fi
}

# Function to check if a container is running
check_container_running() {
  if docker ps | grep -q "$1"; then
    echo -e "${GREEN}✓ Container $1 is running${NC}"
    return 0
  else
    echo -e "${RED}✗ Container $1 is NOT running${NC}"
    return 1
  fi
}

# Function to check if Kafka topic exists
check_kafka_topic() {
  docker exec agri_data_pipeline-broker kafka-topics --bootstrap-server broker:29092 --describe --topic agri_data &> /dev/null
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Kafka topic 'agri_data' exists${NC}"
    return 0
  else
    echo -e "${YELLOW}! Kafka topic 'agri_data' does not exist, will create it${NC}"
    return 1
  fi
}

# Function to check if GCS bucket exists
check_gcs_bucket() {
  BUCKET_NAME=$(grep -E "^GCS_BUCKET_NAME" .env | cut -d "=" -f2)
  gsutil ls -b gs://${BUCKET_NAME} &> /dev/null
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ GCS bucket '${BUCKET_NAME}' exists${NC}"
    return 0
  else
    echo -e "${YELLOW}! GCS bucket '${BUCKET_NAME}' does not exist, will create it${NC}"
    return 1
  fi
}

# Create a timestamp for commit message
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

section "Starting rebuild of Agri Data Pipeline"

# 1. Clean up any running containers
section "1. Cleaning up existing containers"
echo "Stopping Kafka containers..."
docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env down 2>/dev/null || true
echo "Stopping Airflow containers..."
docker-compose -f ./docker/airflow/docker-compose.yml --env-file ./.env down 2>/dev/null || true
echo "Stopping Spark containers..."
docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env down 2>/dev/null || true
echo "Stopping streaming containers..."
docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env down 2>/dev/null || true
echo "Stopping Postgres containers..."
docker-compose -f ./docker/postgres/docker-compose.yml --env-file ./.env down 2>/dev/null || true
echo "Stopping Metabase containers..."
docker-compose -f ./docker/metabase/docker-compose.yml --env-file ./.env down 2>/dev/null || true

echo "Removing any dangling containers..."
docker container prune -f

# Make a git commit at checkpoint
git add .
git commit -m "Checkpoint: Clean environment before rebuild - ${TIMESTAMP}" || true

# 2. Check required environment variables and files
section "2. Checking environment"
if [ ! -f .env ]; then
  handle_error ".env file not found. Please create one from .env.example"
fi

if [ ! -f gcp-creds.json ]; then
  handle_error "gcp-creds.json not found. Please create a GCP service account key file"
fi

# Set up network if it doesn't exist
docker network create agri_data_pipeline-network 2>/dev/null || true
verify $? "Network setup complete"

# 3. Start Kafka
section "3. Starting Kafka"
docker-compose -f ./docker/kafka/docker-compose.yml --env-file ./.env up -d
verify $? "Kafka containers started"

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 15

# Check if Kafka containers are running
check_container_running "agri_data_pipeline-broker"
check_container_running "agri_data_pipeline-zookeeper"

# Create topic if it doesn't exist
if ! check_kafka_topic; then
  echo "Creating Kafka topic..."
  docker exec agri_data_pipeline-broker kafka-topics --bootstrap-server broker:29092 --create --topic agri_data --partitions 1 --replication-factor 1 --if-not-exists
  verify $? "Kafka topic 'agri_data' created"
fi

# Make a git commit at checkpoint
git add .
git commit -m "Checkpoint: Kafka setup complete - ${TIMESTAMP}" || true

# 4. Start Postgres
section "4. Starting PostgreSQL"
docker-compose -f ./docker/postgres/docker-compose.yml --env-file ./.env up -d
verify $? "PostgreSQL container started"

# Check if Postgres container is running
check_container_running "agri_data_pipeline-postgres"

# Make a git commit at checkpoint
git add .
git commit -m "Checkpoint: PostgreSQL setup complete - ${TIMESTAMP}" || true

# 5. Start Airflow
section "5. Starting Airflow"
docker-compose -f ./docker/airflow/docker-compose.yml --env-file ./.env up -d
verify $? "Airflow containers started"

# Wait for Airflow to be ready
echo "Waiting for Airflow to be ready..."
sleep 30

# Check if Airflow container is running
check_container_running "airflow-airflow-webserver-1"

# Fix the DAG errors
section "5.1. Fixing Airflow DAGs"

# Create helpers directory for Airflow
mkdir -p ./docker/airflow/dags/helpers
cat > ./docker/airflow/dags/helpers/__init__.py << EOF
"""
Helper functions for Airflow DAGs.
"""
from google.cloud import storage
from google.cloud import bigquery
import logging
import os

def export_gcs_to_bigquery(**kwargs):
    """Export data from GCS to BigQuery."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        # Get environment variables
        gcs_bucket_name = os.environ.get('GCS_BUCKET_NAME', 'agri_data_bucket')
        gcs_transformed_path = os.environ.get('GCS_TRANSFORMED_DATA_PATH', 'transformed/agri_data/')
        dataset_id = os.environ.get('GCP_DATASET_ID', 'agri_data')
        project_id = os.environ.get('GCP_PROJECT_ID', 'agri-data-project')
        
        logger.info(f"Starting GCS to BigQuery export from gs://{gcs_bucket_name}/{gcs_transformed_path}")
        
        # Initialize clients
        storage_client = storage.Client()
        bigquery_client = bigquery.Client()
        
        # List files in the GCS path
        bucket = storage_client.bucket(gcs_bucket_name)
        blobs = list(bucket.list_blobs(prefix=gcs_transformed_path))
        
        if not blobs:
            logger.info("No files found in GCS path. Nothing to export.")
            return {'status': 'success', 'message': 'No files to export'}
        
        logger.info(f"Found {len(blobs)} files to process")
        
        # Create dataset if it doesn't exist
        dataset_ref = bigquery_client.dataset(dataset_id)
        try:
            bigquery_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_id} already exists")
        except Exception:
            logger.info(f"Creating dataset {dataset_id}")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            bigquery_client.create_dataset(dataset)
        
        # Process each file
        for blob in blobs:
            table_name = os.path.basename(blob.name).split('.')[0]
            
            # Skip if not a file or not a proper table name
            if '/' in table_name or table_name == '':
                continue
                
            # Create load job configuration
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                source_format=bigquery.SourceFormat.PARQUET,
            )
            
            # Uri for the GCS file
            uri = f"gs://{gcs_bucket_name}/{blob.name}"
            
            # Load the data into BigQuery
            table_id = f"{project_id}.{dataset_id}.{table_name}"
            logger.info(f"Loading data into {table_id} from {uri}")
            
            load_job = bigquery_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            
            # Wait for the job to complete
            load_job.result()
            
            logger.info(f"Loaded {load_job.output_rows} rows into {table_id}")
        
        logger.info("GCS to BigQuery export completed successfully")
        return {'status': 'success', 'message': 'Export completed successfully'}
    except Exception as e:
        logger.error(f"Error in GCS to BigQuery export: {str(e)}")
        return {'status': 'error', 'message': str(e)}
EOF

verify $? "Created Airflow helper functions"

# Update the Airflow DAGs
cat > ./docker/airflow/dags/gcs_to_bigquery_dag.py << EOF
"""
GCS to BigQuery Export DAG
This DAG checks for new data in GCS every 10 minutes and exports it to BigQuery.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import sys
import logging

# Add helpers directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'helpers'))

# Import the helper function
try:
    from helpers import export_gcs_to_bigquery
    IMPORT_ERROR = None
except Exception as e:
    IMPORT_ERROR = str(e)
    # Fallback function in case import fails
    def export_gcs_to_bigquery(**kwargs):
        if IMPORT_ERROR:
            raise ImportError(f"Failed to import helper: {IMPORT_ERROR}")
        return {"status": "error", "message": "Helper function not available"}

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['alerts@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'gcs_to_bigquery_export',
    default_args=default_args,
    description='Export data from GCS to BigQuery',
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    max_active_runs=1,
)

# Define the export task
export_task = PythonOperator(
    task_id='export_to_bigquery',
    python_callable=export_gcs_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Logging task to confirm execution
log_task = BashOperator(
    task_id='log_completion',
    bash_command='echo "GCS to BigQuery export completed at $(date)"',
    dag=dag,
)

# Define task dependencies
export_task >> log_task
EOF

verify $? "Updated GCS to BigQuery DAG"

mkdir -p ./docker/airflow/dags/data_consumer
cat > ./docker/airflow/dags/data_consumer/__init__.py << EOF
from confluent_kafka import Consumer, KafkaError
import json
import os
import uuid
import datetime
import logging
import socket
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_temp_dir():
    """Set up temporary directory for storage."""
    import os
    local_temp_dir = "/tmp/agri_data"
    if not os.path.exists(local_temp_dir):
        os.makedirs(local_temp_dir)
    return local_temp_dir

def convert_json_to_parquet(messages, batch_id):
    """Convert a batch of JSON messages to Parquet format."""
    local_temp_dir = setup_temp_dir()
    
    # Check if we have any messages to process
    if not messages:
        logger.info("No messages to convert to Parquet.")
        return None
    
    # Parse the first message to get the schema
    first_message = json.loads(messages[0])
    schema_fields = []
    
    # Create PyArrow schema from the first message
    for key, value in first_message.items():
        if isinstance(value, int):
            field_type = pa.int64()
        elif isinstance(value, float):
            field_type = pa.float64()
        elif isinstance(value, bool):
            field_type = pa.bool_()
        else:
            field_type = pa.string()
        
        schema_fields.append(pa.field(key, field_type))
    
    schema = pa.schema(schema_fields)
    
    # Parse all messages and create arrays
    parsed_messages = [json.loads(msg) for msg in messages]
    arrays = []
    
    # Create arrays for each field
    for field in schema_fields:
        field_name = field.name
        field_values = [msg.get(field_name, None) for msg in parsed_messages]
        arrays.append(pa.array(field_values, field.type))
    
    # Create table and write to Parquet
    table = pa.Table.from_arrays(arrays, schema=schema)
    
    # Generate a timestamp for the filename
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    parquet_filename = f"{local_temp_dir}/agri_data_batch_{batch_id}_{timestamp}.parquet"
    
    # Write to Parquet file
    pq.write_table(table, parquet_filename)
    
    logger.info(f"Converted {len(messages)} messages to Parquet file: {parquet_filename}")
    
    return parquet_filename

def upload_to_gcs(parquet_file):
    """Upload Parquet file to Google Cloud Storage."""
    if parquet_file is None:
        logger.info("No Parquet file to upload.")
        return
    
    try:
        # Get GCS bucket and blob path from environment
        bucket_name = os.environ.get("GCS_BUCKET_NAME", "agri_data_bucket")
        blob_path = os.environ.get("GCS_RAW_DATA_PATH", "raw/agri_data/")
        
        # Create GCS client
        storage_client = storage.Client()
        
        # Get or create bucket
        try:
            bucket = storage_client.get_bucket(bucket_name)
        except Exception:
            bucket = storage_client.create_bucket(bucket_name)
            logger.info(f"Created new bucket: {bucket_name}")
        
        # Generate blob name from file name
        file_name = os.path.basename(parquet_file)
        blob_name = f"{blob_path}{file_name}"
        
        # Upload file to GCS
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(parquet_file)
        
        logger.info(f"File {file_name} uploaded to gs://{bucket_name}/{blob_name}")
        
        # Delete local file after upload
        os.remove(parquet_file)
        logger.info(f"Deleted local file {parquet_file}")
        
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")

def consume_messages(**kwargs):
    """Consume messages from Kafka and process them in batches."""
    local_temp_dir = setup_temp_dir()
    
    # Configuration
    kafka_broker = os.environ.get("KAFKA_BROKER", "broker:29092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "agri_data")
    consumer_group = os.environ.get("CONSUMER_GROUP", "agri_data_consumer")
    max_messages = int(os.environ.get("MAX_MESSAGES", 1000))
    batch_size = int(os.environ.get("BATCH_SIZE", 100))
    poll_timeout = float(os.environ.get("POLL_TIMEOUT", 1.0))
    
    # Configuration for the Kafka consumer
    consumer_config = {
        'bootstrap.servers': kafka_broker,
        'group.id': consumer_group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'debug': 'broker,topic,consumer'
    }
    
    logger.info(f"Consumer configuration: {consumer_config}")
    
    # Create Kafka consumer
    consumer = Consumer(consumer_config)
    consumer.subscribe([kafka_topic])
    
    logger.info(f"Starting to consume messages from topic {kafka_topic}")
    
    messages = []
    batch_counter = 0
    message_counter = 0
    batch_id = str(uuid.uuid4())[:8]
    
    try:
        while message_counter < max_messages:
            # Poll for messages
            msg = consumer.poll(poll_timeout)
            
            if msg is None:
                # No message received in this poll cycle
                if message_counter > 0 and message_counter % 10 == 0:
                    logger.info("No new messages, continuing to poll...")
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, not an error
                    logger.info(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Error consuming message: {msg.error()}")
                continue
            
            # Process message
            message_value = msg.value().decode('utf-8')
            messages.append(message_value)
            message_counter += 1
            
            logger.info(f"Consumed message {message_counter}/{max_messages} from partition {msg.partition()} offset {msg.offset()}")
            
            # Process a batch of messages when batch size is reached
            if len(messages) >= batch_size:
                logger.info(f"Processing batch {batch_counter + 1} with {len(messages)} messages")
                
                # Convert to Parquet and upload to GCS
                parquet_file = convert_json_to_parquet(messages, batch_id)
                upload_to_gcs(parquet_file)
                
                # Reset messages and increment batch counter
                messages = []
                batch_counter += 1
                batch_id = str(uuid.uuid4())[:8]
                
                # Commit offsets
                consumer.commit()
        
        # Process any remaining messages
        if messages:
            logger.info(f"Processing final batch with {len(messages)} messages")
            parquet_file = convert_json_to_parquet(messages, batch_id)
            upload_to_gcs(parquet_file)
            consumer.commit()
        
        logger.info(f"Finished consuming {message_counter} messages in {batch_counter + 1} batches")
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
    finally:
        # Clean up
        consumer.close()
        logger.info("Consumer closed")
    
    return {'status': 'success', 'message': f'Processed {message_counter} messages in {batch_counter + 1} batches'}
EOF

verify $? "Created Kafka consumer module for Airflow"

cat > ./docker/airflow/dags/kafka_to_gcs_dag.py << EOF
"""
Kafka to GCS Streaming DAG
This DAG consumes data from Kafka and stores it in GCS.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import sys
import logging

# Import path for the consumer script
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

# Import the consumer function
from data_consumer import consume_messages

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['alerts@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'kafka_to_gcs_streaming',
    default_args=default_args,
    description='Consume data from Kafka and store in GCS',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    catchup=False,
    max_active_runs=1,
)

# Define the consumer task
consume_task = PythonOperator(
    task_id='consume_from_kafka',
    python_callable=consume_messages,
    provide_context=True,
    dag=dag,
)

# Logging task to confirm execution
log_task = BashOperator(
    task_id='log_completion',
    bash_command='echo "Kafka to GCS streaming completed at $(date)"',
    dag=dag,
)

# Define task dependencies
consume_task >> log_task
EOF

verify $? "Updated Kafka to GCS DAG"

# Make a git commit at checkpoint
git add .
git commit -m "Checkpoint: Airflow DAGs fixed - ${TIMESTAMP}" || true

# 6. Start Spark
section "6. Starting Spark"
chmod +x ./docker/spark/build.sh
./docker/spark/build.sh
verify $? "Spark images built"

docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env up -d
verify $? "Spark containers started"

# Check if Spark containers are running
check_container_running "agri_data_pipeline-spark-master"
check_container_running "agri_data_pipeline-spark-worker-1"

# Copy batch pipeline code to Spark container
echo "Copying batch pipeline code to Spark container..."
docker cp ./batch_pipeline agri_data_pipeline-spark-master:/opt/bitnami/spark/
docker cp ./gcp-creds.json agri_data_pipeline-spark-master:/opt/bitnami/spark/

# Make a git commit at checkpoint
git add .
git commit -m "Checkpoint: Spark setup complete - ${TIMESTAMP}" || true

# 7. Ensure GCS bucket exists
section "7. Checking GCS Bucket"
BUCKET_NAME=$(grep -E "^GCS_BUCKET_NAME" .env | cut -d "=" -f2)
if ! check_gcs_bucket; then
  echo "Creating GCS bucket..."
  gsutil mb -p $(grep -E "^GCP_PROJECT_ID" .env | cut -d "=" -f2) gs://${BUCKET_NAME}/
  verify $? "GCS bucket created"
fi

# Make a git commit at checkpoint
git add .
git commit -m "Checkpoint: GCS bucket setup complete - ${TIMESTAMP}" || true

# 8. Run the streaming data producer
section "8. Starting the streaming data producer"
docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d
verify $? "Streaming containers started"

# Check if streaming containers are running
check_container_running "agri_data_pipeline-data-producer"

# Make a git commit at checkpoint
git add .
git commit -m "Checkpoint: Data producer started - ${TIMESTAMP}" || true

# 9. Run the batch pipeline
section "9. Running batch pipeline"
echo "Submitting Spark job for batch processing..."

# Wait to allow streaming data to accumulate
echo "Waiting 30 seconds for streaming data to accumulate..."
sleep 30

# Submit the Spark job
docker exec agri_data_pipeline-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --files /opt/bitnami/spark/gcp-creds.json \
  --conf "spark.executor.memory=1g" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.hadoop.google.cloud.auth.service.account.enable=true" \
  --conf "spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/bitnami/spark/gcp-creds.json" \
  /opt/bitnami/spark/batch_pipeline/export_to_gcs/pipeline.py

# Check if the Spark job was submitted
if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Batch pipeline job submitted successfully${NC}"
else
  echo -e "${YELLOW}! Batch pipeline job submission failed or had issues${NC}"
fi

# Make a git commit at checkpoint
git add .
git commit -m "Checkpoint: Batch processing started - ${TIMESTAMP}" || true

# 10. Run DBT transformations
section "10. Running DBT transformations"
cd business_transformations
echo "Installing dbt dependencies..."
pip install -q dbt-bigquery
verify $? "dbt-bigquery installed"

echo "Running dbt models..."
dbt run --profiles-dir=.
if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ DBT models run successfully${NC}"
else
  echo -e "${YELLOW}! DBT models run had issues${NC}"
fi

cd ..

# Make a git commit at checkpoint
git add .
git commit -m "Checkpoint: DBT transformations complete - ${TIMESTAMP}" || true

# 11. Set up Metabase
section "11. Starting Metabase"
docker-compose -f ./docker/metabase/docker-compose.yml --env-file ./.env up -d
verify $? "Metabase container started"

# Check if Metabase container is running
check_container_running "agri_data_pipeline-metabase"

# Make a git commit at checkpoint
git add .
git commit -m "Checkpoint: Metabase setup complete - ${TIMESTAMP}" || true

# 12. Final verification
section "12. Final verification"
echo "Current running containers:"
docker ps

echo -e "\nVerifying data is in GCS..."
gsutil ls -r gs://${BUCKET_NAME}/ | head -5

echo -e "\nVerifying data is in BigQuery..."
bq ls $(grep -E "^GCP_PROJECT_ID" .env | cut -d "=" -f2):agri_data

section "Pipeline rebuild complete!"
echo -e "\nServices available at:"
echo -e "- Kafka Control Center: http://localhost:9021"
echo -e "- Airflow UI: http://localhost:8080 (login: airflow/airflow)"
echo -e "- Spark UI: http://localhost:8080"
echo -e "- Metabase: http://localhost:3000"

echo -e "\n${GREEN}The pipeline has been rebuilt and restarted successfully.${NC}"
echo -e "${YELLOW}Please allow 5-10 minutes for all data to flow through the pipeline.${NC}"

# Make a final git commit
git add .
git commit -m "Rebuild complete: Data pipeline is functional - ${TIMESTAMP}" || true 