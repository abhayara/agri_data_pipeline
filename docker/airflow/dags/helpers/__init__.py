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
            dataset.location = "asia-south1"
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
