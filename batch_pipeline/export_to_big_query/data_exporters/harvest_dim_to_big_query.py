from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os

def export_data_to_big_query(df):
    """
    Function to export harvest dimension data to BigQuery.
    
    Args:
        df: pandas DataFrame containing harvest dimension data
    """
    if df.empty:
        print("DataFrame is empty. Nothing to export.")
        return
    
    # Set up credentials
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', './gcp-creds.json')
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    
    # Initialize BigQuery client
    client = bigquery.Client(credentials=credentials)
    
    # Define dataset and table parameters
    project_id = os.getenv('GCP_PROJECT_ID', 'agri-data-454414')
    dataset_id = os.getenv('GCP_DATASET_ID', 'agri_data')
    table_id = f"{project_id}.{dataset_id}.dim_harvest"
    
    # Load the data to BigQuery
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",  # Overwrite the table if it exists
    )
    
    print(f"Exporting {len(df)} records to BigQuery table {table_id}")
    
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    
    # Wait for the load job to complete
    job.result()
    
    print(f"Loaded {job.output_rows} rows to {table_id}")
    
if __name__ == "__main__":
    # Import load function from the corresponding loader
    from data_loaders.load_harvest_dimension import load_from_google_cloud_storage
    
    # Load data from GCS
    df = load_from_google_cloud_storage()
    
    # Export data to BigQuery
    export_data_to_big_query(df) 