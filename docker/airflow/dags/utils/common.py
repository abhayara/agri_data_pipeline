"""
Common utility functions for the Airflow DAGs.
"""
import os
import json
import logging
from google.cloud import storage
from kafka import KafkaConsumer, KafkaProducer
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configure logging
logger = logging.getLogger("airflow.task")

def get_kafka_producer():
    """
    Returns a Kafka producer instance.
    """
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def get_kafka_consumer(topic=None, group_id=None):
    """
    Returns a Kafka consumer instance.
    """
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    topic = topic or os.getenv('KAFKA_TOPIC', 'agri_data')
    group_id = group_id or os.getenv('KAFKA_GROUP_ID', 'agri_data_consumer')
    auto_offset_reset = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )

def get_postgres_hook():
    """
    Returns a Postgres hook instance.
    """
    return PostgresHook(
        postgres_conn_id='postgres_default',
        schema=os.getenv('POSTGRES_DBNAME', 'agri_db')
    )

def get_storage_client():
    """
    Returns a Google Cloud Storage client instance.
    """
    return storage.Client()

def upload_to_gcs(bucket_name, blob_name, data_file):
    """
    Uploads a file to Google Cloud Storage.
    """
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(data_file)
    logger.info(f"Uploaded {data_file} to {blob_name}")
    return blob.public_url 