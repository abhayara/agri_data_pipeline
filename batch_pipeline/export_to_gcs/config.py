"""
Configuration for the agricultural data batch pipeline.
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# GCS configuration
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "agri_data_bucket")
RAW_DATA_PATH = os.getenv("GCS_RAW_DATA_PATH", "raw/agri_data/")
TRANSFORMED_DATA_PATH = os.getenv("GCS_TRANSFORMED_DATA_PATH", "transformed/agri_data/")

# GCP configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "agri-data-454414")
DATASET_ID = os.getenv("GCP_DATASET_ID", "agri_data")

# Dimension tables
DIMENSION_TABLES = ['farm', 'crop', 'weather', 'soil', 'harvest']

# Fact tables
FACT_TABLES = ['production', 'yield', 'sustainability']

# Credentials and connector paths
credentials_path = './gcp-creds.json'
jar_file_path = "./docker/spark/jar_files/gcs-connector-hadoop3-2.2.5.jar" 