"""
Helper module for Airflow DAGs
Contains utility functions for GCS to BigQuery export
"""

import os
import logging
from google.cloud import storage
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def export_gcs_to_bigquery(**kwargs):
    """
    Export data from GCS to BigQuery
    """
    try:
        # Get environment variables
        project_id = os.getenv('GCP_PROJECT_ID', 'agri-data-project')
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')
        dataset_id = os.getenv('GCP_DATASET_ID', 'agri_data')
        source_path = os.getenv('GCS_TRANSFORMED_DATA_PATH', 'transformed/agri_data/')
        
        logger.info(f"Starting GCS to BigQuery export from gs://{bucket_name}/{source_path}")
        
        # Initialize clients
        storage_client = storage.Client()
        bigquery_client = bigquery.Client()
        
        # List files in the bucket
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=source_path))
        
        logger.info(f"Found {len(blobs)} files to process")
        
        # Create dataset if it doesn't exist
        dataset_ref = bigquery_client.dataset(dataset_id)
        try:
            bigquery_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_id} exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            bigquery_client.create_dataset(dataset)
            logger.info(f"Created dataset {dataset_id}")
        
        # Load each file to BigQuery
        for blob in blobs:
            # Extract table name from filename
            file_name = os.path.basename(blob.name)
            table_name = file_name.split('.')[0]
            
            # Create load job config
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                autodetect=True
            )
            
            # Start load job
            uri = f"gs://{bucket_name}/{blob.name}"
            table_id = f"{project_id}.{dataset_id}.{table_name}"
            load_job = bigquery_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            
            logger.info(f"Loading {uri} to {table_id}")
            load_job.result()  # Wait for job to complete
            
            logger.info(f"Successfully loaded {uri} to {table_id}")
        
        return {"status": "success", "files_processed": len(blobs)}
        
    except Exception as e:
        logger.error(f"Error exporting to BigQuery: {e}")
        raise 