import json
import logging
import tempfile
from typing import Dict, List, Any
from google.cloud import storage
from utils.constants import GCS_CREDENTIALS_PATH
from airflow.decorators import task
import os

logger = logging.getLogger(__name__)

class GCSExporter:
    def __init__(self, bucket_name):
        """Initialize GCS exporter with the bucket name."""
        self.bucket_name = bucket_name
        self.client = storage.Client()
        
    def export_to_gcs(self, data, destination_blob_name):
        """
        Export data to Google Cloud Storage.
        
        Args:
            data: Data to export (could be a string, dict, or list)
            destination_blob_name: Name of the destination blob in GCS
        
        Returns:
            URL of the uploaded file
        """
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # If data is a file path
        if isinstance(data, str) and os.path.isfile(data):
            blob.upload_from_filename(data)
        else:
            # Assume it's JSON-serializable data
            import json
            blob.upload_from_string(json.dumps(data))
        
        return f"gs://{self.bucket_name}/{destination_blob_name}"

@task
def export_to_gcs(local_file_path, bucket_name, destination_blob_name):
    """
    Airflow task to export data to GCS
    
    Args:
        local_file_path: Path to the local file
        bucket_name: GCS bucket name
        destination_blob_name: Name of the destination blob in GCS
    
    Returns:
        URL of the uploaded file
    """
    exporter = GCSExporter(bucket_name=bucket_name)
    return exporter.export_to_gcs(local_file_path, destination_blob_name)

def upload_to_gcs(data: List[Dict[str, Any]], bucket_name: str, blob_path: str) -> str:
    """
    Upload data to Google Cloud Storage
    
    Args:
        data: List of dictionaries to upload
        bucket_name: GCS bucket name
        blob_path: Path within the bucket
        
    Returns:
        URI of the uploaded file
    """
    try:
        # Initialize GCS client
        storage_client = storage.Client.from_service_account_json(GCS_CREDENTIALS_PATH)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        # Write data to temporary file
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.json') as temp_file:
            json.dump(data, temp_file, ensure_ascii=False, indent=2)
            temp_file.flush()
            temp_file.seek(0)
            
            # Upload file content to GCS
            logger.info(f"Uploading data to gs://{bucket_name}/{blob_path}")
            blob.upload_from_file(temp_file)
            
        gcs_uri = f"gs://{bucket_name}/{blob_path}"
        logger.info(f"Successfully uploaded data to {gcs_uri}")
        
        return gcs_uri
        
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")
        raise
