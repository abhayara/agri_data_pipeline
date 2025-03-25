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
import pandas as pd
import os
import logging
import json
from kafka import KafkaConsumer
from google.cloud import storage
import time

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'agri_data_pipeline',
    default_args=default_args,
    description='Pipeline to process agricultural data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
)

# Configure logging
logger = logging.getLogger("airflow.task")

# Function to consume data from Kafka and save to GCS
def consume_from_kafka_save_to_gcs(**kwargs):
    # Kafka configuration from environment variables
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    topic = os.getenv('KAFKA_TOPIC', 'agri_data')
    group_id = os.getenv('KAFKA_GROUP_ID', 'agri_data_consumer')
    auto_offset_reset = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    
    # GCS configuration
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')
    blob_path = os.getenv('GCS_TEMP_BLOB_PATH', 'raw/agri_data/')
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # Timeout after 10 seconds of no messages
    )
    
    # Consume messages
    messages = []
    try:
        for message in consumer:
            logger.info(f"Received message: {message.value}")
            messages.append(message.value)
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
    finally:
        consumer.close()
    
    if not messages:
        logger.info("No messages received from Kafka")
        return
    
    # Convert messages to DataFrame
    df = pd.DataFrame(messages)
    
    # Upload to GCS
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    file_path = f"/tmp/agri_data_{timestamp}.csv"
    df.to_csv(file_path, index=False)
    
    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{blob_path}agri_data_{timestamp}.csv")
    
    blob.upload_from_filename(file_path)
    logger.info(f"Uploaded {len(messages)} messages to {blob.name}")
    
    # Clean up temporary file
    os.remove(file_path)

# Function to process dimension tables
def process_dimension_tables(**kwargs):
    # Get list of dimension tables
    dimension_tables = os.getenv('DIMENSION_TABLES', 'farm,crop,weather,soil,harvest').split(',')
    
    # Configuration
    gcs_bucket = os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')
    
    # Process each dimension table
    for table in dimension_tables:
        logger.info(f"Processing dimension table: {table}")
        
        # Example: Here you would extract data from the main dataset and create dimension tables
        # For demonstration, we'll create simple dimension tables with dummy data
        
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
            continue
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        file_path = f"/tmp/{table}_{timestamp}.csv"
        df.to_csv(file_path, index=False)
        
        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(gcs_bucket)
        blob = bucket.blob(f"dimensions/{table}/{table}_{timestamp}.csv")
        
        blob.upload_from_filename(file_path)
        logger.info(f"Uploaded dimension table {table} to {blob.name}")
        
        # Clean up temporary file
        os.remove(file_path)

# Function to create tables in BigQuery
def create_bigquery_schema(**kwargs):
    # BigQuery configuration
    project_id = os.getenv('GCP_PROJECT_ID', 'agri-data-454414')
    dataset_id = os.getenv('GCP_DATASET_ID', 'agri_data')
    
    # Schema for each table would be defined here
    # For now, we'll use auto-detect schema in the BigQuery operators

# Create task to consume Kafka data and save to GCS
kafka_to_gcs_task = PythonOperator(
    task_id='consume_kafka_to_gcs',
    python_callable=consume_from_kafka_save_to_gcs,
    dag=dag,
)

# Create task to process dimension tables
process_dimensions_task = PythonOperator(
    task_id='process_dimension_tables',
    python_callable=process_dimension_tables,
    dag=dag,
)

# Create BigQuery dataset
create_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    dataset_id=os.getenv('GCP_DATASET_ID', 'agri_data'),
    project_id=os.getenv('GCP_PROJECT_ID', 'agri-data-454414'),
    location=os.getenv('BQ_LOCATION', 'US'),
    exists_ok=True,
    dag=dag,
)

# Task dependencies
kafka_to_gcs_task >> process_dimensions_task >> create_dataset_task 