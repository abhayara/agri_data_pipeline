"""
Configuration for the agricultural data batch pipeline.
"""

# GCS bucket and paths
GCS_BUCKET = "agri_data_bucket"
RAW_DATA_PATH = "raw/agri_data/"
TRANSFORMED_DATA_PATH = "transformed/agri_data/"

# Dimension tables
DIMENSION_TABLES = ['farm', 'crop', 'weather', 'soil', 'harvest']

# Fact tables
FACT_TABLES = ['production', 'yield', 'sustainability']

# Credentials and connector paths
credentials_path = './gcp-creds.json'
jar_file_path = "./docker/spark/jar_files/gcs-connector-hadoop3-2.2.5.jar" 