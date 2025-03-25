from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import os

def load_from_google_cloud_storage():
    """
    Function to load farm dimension data from Google Cloud Storage.
    
    Returns:
        pandas.DataFrame: DataFrame containing farm dimension data
    """
    # Set up credentials
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', './gcp-creds.json')
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    
    # Initialize clients
    storage_client = storage.Client(credentials=credentials)
    
    # Define bucket and object parameters
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')
    object_prefix = 'transformed/agri_data/farm_dimension'
    
    # List all objects with the given prefix
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=object_prefix))
    
    print(f"Found {len(blobs)} files for farm dimension.")
    
    # Read and concatenate all parquet files
    dfs = []
    for blob in blobs:
        if blob.name.endswith('.parquet'):
            # Download to temporary file
            temp_file = f"/tmp/{os.path.basename(blob.name)}"
            blob.download_to_filename(temp_file)
            
            # Read parquet file and append to list
            df = pd.read_parquet(temp_file)
            dfs.append(df)
            
            # Clean up temporary file
            os.remove(temp_file)
    
    # Concatenate all dataframes
    if dfs:
        concatenated_df = pd.concat(dfs, ignore_index=True)
        print(f"Loaded {len(concatenated_df)} farm dimension records.")
        return concatenated_df
    else:
        print("No data loaded for farm dimension.")
        return pd.DataFrame()

if __name__ == "__main__":
    # For testing purposes
    df = load_from_google_cloud_storage()
    print(df.head()) 