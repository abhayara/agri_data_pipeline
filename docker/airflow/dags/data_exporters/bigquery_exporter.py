import logging
import pandas as pd
from typing import Dict, List, Any
from google.cloud import bigquery
from utils.constants import GCS_CREDENTIALS_PATH

logger = logging.getLogger(__name__)

class BigQueryExporter:
    def __init__(self, project_id, dataset_id):
        """Initialize BigQuery exporter with project and dataset IDs."""
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def export_to_bigquery(self, data, table_name, if_exists='replace'):
        """
        Export data to BigQuery table.
        
        Args:
            data: Pandas DataFrame or list of dicts to export
            table_name: Name of the destination table
            if_exists: What to do if the table exists ('fail', 'replace', or 'append')
            
        Returns:
            Number of rows exported
        """
        # Convert to DataFrame if it's not already
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)
        
        # Full table reference
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        # Use pandas_gbq to handle the upload
        data.to_gbq(
            destination_table=f"{self.dataset_id}.{table_name}",
            project_id=self.project_id,
            if_exists=if_exists
        )
        
        return len(data)

def export_to_bigquery(
    data: List[Dict[str, Any]],
    project_id: str,
    dataset_id: str,
    table_id: str,
    location: str = "US",
    write_disposition: str = "WRITE_TRUNCATE"
) -> str:
    """
    Export data to BigQuery
    
    Args:
        data: List of dictionaries to export
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        location: BigQuery dataset location
        write_disposition: How to handle existing data
        
    Returns:
        Fully qualified table ID
    """
    try:
        # Initialize BigQuery client
        client = bigquery.Client.from_service_account_json(
            GCS_CREDENTIALS_PATH,
            project=project_id,
            location=location
        )
        
        # Create dataset if it doesn't exist
        dataset_ref = f"{project_id}.{dataset_id}"
        try:
            client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
        except Exception:
            logger.info(f"Creating dataset {dataset_ref}")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            client.create_dataset(dataset, exists_ok=True)
        
        # Convert data to DataFrame
        df = pd.DataFrame(data)
        
        # Get fully qualified table ID
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        # Load data to BigQuery
        logger.info(f"Loading data to {table_ref}")
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True
        )
        
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        
        job.result()  # Wait for job to complete
        
        logger.info(f"Successfully loaded data to {table_ref}")
        return table_ref
        
    except Exception as e:
        logger.error(f"Error exporting to BigQuery: {e}")
        raise
