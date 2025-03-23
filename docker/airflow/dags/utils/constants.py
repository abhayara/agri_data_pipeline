import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "agri_data")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "agri_data_consumer")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

# Google Cloud Storage Configuration
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_TEMP_BLOB_PATH = os.getenv("GCS_TEMP_BLOB_PATH", "raw/agri_data/")
GCS_CREDENTIALS_PATH = os.getenv("GCS_CREDENTIALS_PATH", "/opt/airflow/gcp-creds.json")

# BigQuery Configuration
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "agri_data_dataset")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# Dimension Tables Configuration
DIMENSION_TABLES = os.getenv("DIMENSION_TABLES", "farm,crop,weather,soil,harvest").split(",")
