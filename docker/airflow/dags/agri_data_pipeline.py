"""
Airflow DAG to process agricultural data from Kafka to GCS and BigQuery.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
import pandas as pd
import os
import logging
import json
import time
import traceback
from kafka import KafkaConsumer
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,  # Increase retries
    'retry_delay': timedelta(minutes=2),  # Exponential backoff
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30),
}

# Create the DAG
dag = DAG(
    'agri_data_pipeline',
    default_args=default_args,
    description='Pipeline to process agricultural data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['agricultural_data', 'streaming'],
)

# Start task
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# End task
end_task = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.ALL_DONE,  # Run even if some tasks fail
    dag=dag,
)

# Create a task to check service dependencies
def check_service_dependencies(**kwargs):
    """Check if all required services are available."""
    try:
        # Check Kafka
        from kafka.admin import KafkaAdminClient
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        
        # Check if topic exists, create if it doesn't
        topic = os.getenv('KAFKA_TOPIC', 'agri_data')
        topics = admin_client.list_topics()
        if topic not in topics:
            from kafka.admin import NewTopic
            topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
        
        # Check GCS
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')
        storage_client = storage.Client()
        try:
            bucket = storage_client.get_bucket(bucket_name)
        except Exception as e:
            logger.warning(f"GCS bucket does not exist: {e}")
            bucket = storage_client.create_bucket(bucket_name)
            
        logger.info("All service dependencies available.")
        return True
    except Exception as e:
        logger.error(f"Service dependency check failed: {e}")
        logger.error(traceback.format_exc())
        raise

# Add dependency check task
check_dependencies_task = PythonOperator(
    task_id='check_dependencies',
    python_callable=check_service_dependencies,
    retries=10,  # More retries for dependency check
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

# Function to consume data from Kafka and save to GCS with retry mechanism
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60))
def consume_from_kafka_save_to_gcs(**kwargs):
    """Consume data from Kafka and save to GCS with retry mechanism."""
    # Kafka configuration from environment variables
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    topic = os.getenv('KAFKA_TOPIC', 'agri_data')
    group_id = os.getenv('KAFKA_GROUP_ID', 'agri_data_consumer')
    auto_offset_reset = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    
    # GCS configuration
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')
    blob_path = os.getenv('GCS_TEMP_BLOB_PATH', 'raw/agri_data/')
    
    # Create Kafka consumer with error handling
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=30000  # Increased timeout to 30 seconds
        )
        
        # Consume messages
        messages = []
        try:
            logger.info(f"Starting to consume messages from topic {topic}")
            for message in consumer:
                logger.info(f"Received message: {message.value}")
                messages.append(message.value)
                # Break if we have enough messages to process in one batch
                if len(messages) >= 10000:
                    logger.info("Reached batch size limit, processing current batch")
                    break
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            consumer.close()
        
        if not messages:
            logger.warning("No messages received from Kafka, retrying...")
            raise Exception("No messages received from Kafka")
        
        # Convert messages to DataFrame with error handling
        try:
            df = pd.DataFrame(messages)
            
            # Generate timestamp for file naming
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            file_path = f"/tmp/agri_data_{timestamp}.csv"
            
            # Save to local temp file
            df.to_csv(file_path, index=False)
            logger.info(f"Saved {len(messages)} messages to local file {file_path}")
            
            # Upload to GCS with retry
            try:
                storage_client = storage.Client()
                bucket = storage_client.bucket(bucket_name)
                
                # Create blob path if it doesn't exist
                blob_name = f"{blob_path}agri_data_{timestamp}.csv"
                blob = bucket.blob(blob_name)
                
                # Upload with retry
                for attempt in range(3):
                    try:
                        blob.upload_from_filename(file_path)
                        logger.info(f"Uploaded {len(messages)} messages to {blob.name}")
                        break
                    except Exception as e:
                        if attempt < 2:  # Retry up to 3 times
                            logger.warning(f"Upload attempt {attempt+1} failed: {e}. Retrying...")
                            time.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            raise
                
                # Clean up temporary file
                os.remove(file_path)
                return f"gs://{bucket_name}/{blob_name}"
            except Exception as e:
                logger.error(f"GCS upload error: {e}")
                logger.error(traceback.format_exc())
                # Don't remove the file if upload failed
                raise
        except Exception as e:
            logger.error(f"Error processing messages: {e}")
            logger.error(traceback.format_exc())
            raise
    except Exception as e:
        logger.error(f"Kafka consumer initialization error: {e}")
        logger.error(traceback.format_exc())
        raise

# Create task to consume Kafka data and save to GCS with improved error handling
kafka_to_gcs_task = PythonOperator(
    task_id='consume_kafka_to_gcs',
    python_callable=consume_from_kafka_save_to_gcs,
    retries=5,
    retry_delay=timedelta(minutes=2),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
    dag=dag,
)

# Function to process dimension tables with error handling
def process_dimension_tables(**kwargs):
    """Process dimension tables with comprehensive error handling."""
    # Get list of dimension tables
    dimension_tables = os.getenv('DIMENSION_TABLES', 'farm,crop,weather,soil,harvest').split(',')
    
    # Configuration
    gcs_bucket = os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')
    
    results = {}
    
    # Process each dimension table
    for table in dimension_tables:
        try:
            logger.info(f"Processing dimension table: {table}")
            
            # Create a simple DataFrame for each dimension
            if table == 'farm':
                data = {
                    'farm_id': [f'FARM{i}' for i in range(1, 11)],
                    'farm_name': [f'Farm {i}' for i in range(1, 11)],
                    'location': [f'Location {i}' for i in range(1, 11)],
                    'size_acres': [i * 10 for i in range(1, 11)]
                }
            elif table == 'crop':
                data = {
                    'crop_id': [f'CROP{i}' for i in range(1, 11)],
                    'crop_name': [f'Crop {i}' for i in range(1, 11)],
                    'crop_type': ['Grain', 'Vegetable', 'Fruit', 'Fiber', 'Oilseed'] * 2
                }
            elif table == 'weather':
                data = {
                    'weather_id': [f'WTH{i}' for i in range(1, 11)],
                    'temperature': [i * 5 for i in range(1, 11)],
                    'humidity': [i * 10 for i in range(1, 11)],
                    'rainfall': [i * 2 for i in range(1, 11)]
                }
            elif table == 'soil':
                data = {
                    'soil_id': [f'SOIL{i}' for i in range(1, 11)],
                    'soil_type': ['Clay', 'Sandy', 'Loam', 'Silt', 'Peaty'] * 2,
                    'pH': [i * 0.5 + 5 for i in range(1, 11)],
                    'fertility': [i for i in range(1, 11)]
                }
            elif table == 'harvest':
                data = {
                    'harvest_id': [f'HRV{i}' for i in range(1, 11)],
                    'farm_id': [f'FARM{i}' for i in range(1, 11)],
                    'crop_id': [f'CROP{i}' for i in range(1, 11)],
                    'harvest_date': [(datetime.now() - timedelta(days=i*10)).strftime('%Y-%m-%d') for i in range(1, 11)],
                    'yield_amount': [i * 100 for i in range(1, 11)]
                }
            else:
                logger.warning(f"Unrecognized dimension table: {table}, skipping")
                continue
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Save to CSV
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            file_path = f"/tmp/{table}_{timestamp}.csv"
            
            try:
                df.to_csv(file_path, index=False)
                
                # Upload to GCS with retry logic
                storage_client = storage.Client()
                bucket = storage_client.bucket(gcs_bucket)
                blob_name = f"dimensions/{table}/{table}_{timestamp}.csv"
                blob = bucket.blob(blob_name)
                
                for attempt in range(3):
                    try:
                        blob.upload_from_filename(file_path)
                        logger.info(f"Uploaded dimension table {table} to {blob.name}")
                        results[table] = f"gs://{gcs_bucket}/{blob_name}"
                        break
                    except Exception as e:
                        if attempt < 2:
                            logger.warning(f"Upload attempt {attempt+1} for {table} failed: {e}. Retrying...")
                            time.sleep(2 ** attempt)
                        else:
                            raise
                
                # Clean up temporary file
                os.remove(file_path)
            except Exception as e:
                logger.error(f"Error processing dimension table {table}: {e}")
                logger.error(traceback.format_exc())
                # Continue with next table instead of failing completely
                continue
                
        except Exception as e:
            logger.error(f"Error processing dimension table {table}: {e}")
            logger.error(traceback.format_exc())
            # Continue with next table instead of failing completely
            continue
    
    # Raise exception if no tables were processed successfully
    if not results:
        raise Exception("Failed to process any dimension tables")
        
    return results

# Create task to process dimension tables
process_dimensions_task = PythonOperator(
    task_id='process_dimension_tables',
    python_callable=process_dimension_tables,
    retries=3,
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

# Create BigQuery dataset
create_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    dataset_id=os.getenv('GCP_DATASET_ID', 'agri_data'),
    project_id=os.getenv('GCP_PROJECT_ID', 'agri-data-454414'),
    exists_ok=True,
    dag=dag,
)

# Set task dependencies with improved flow
start_task >> check_dependencies_task >> kafka_to_gcs_task >> process_dimensions_task >> create_dataset_task >> end_task 