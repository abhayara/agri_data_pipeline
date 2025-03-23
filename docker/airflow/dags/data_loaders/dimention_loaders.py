import pandas as pd
import os
from airflow.decorators import task
from google.cloud import storage
import json
import tempfile
from utils.constants import GCS_BUCKET_NAME, DATA_DIR

def _load_latest_data_from_gcs(bucket_name=GCS_BUCKET_NAME, prefix='kafka_data_'):
    """Load the latest data from GCS for processing."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    # Sort blobs by name to find the latest
    latest_blob = sorted(blobs, key=lambda x: x.name, reverse=True)[0] if blobs else None
    
    if not latest_blob:
        return []
    
    # Download blob content to a temporary file
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
        latest_blob.download_to_file(temp_file)
        temp_file_path = temp_file.name
    
    # Read the downloaded JSON file
    with open(temp_file_path, 'r') as f:
        data = json.load(f)
    
    return data

def load_dimension_data(filename):
    """
    Generic function to load dimension data from JSON files
    """
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        # Return empty dataframe if file doesn't exist
        return pd.DataFrame()
    
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    return pd.DataFrame(data)

def load_customer_dim():
    """Load customer dimension data"""
    return load_dimension_data('customer_dim.json')

def load_department_dim():
    """Load department dimension data"""
    return load_dimension_data('department_dim.json')

def load_location_dim():
    """Load location dimension data"""
    return load_dimension_data('location_dim.json')

def load_metadata_dim():
    """Load metadata dimension data"""
    return load_dimension_data('metadata_dim.json')

def load_order_dim():
    """Load order dimension data"""
    return load_dimension_data('order_dim.json')

@task
def load_customer_dimension(input_file, **kwargs):
    """Load customer dimension data from source file"""
    df = pd.read_json(input_file)
    customers = df[['customer_id', 'customer_name', 'customer_email', 'customer_address']].drop_duplicates()
    output_path = os.path.join(os.path.dirname(input_file), 'customer_dimension.csv')
    customers.to_csv(output_path, index=False)
    return output_path

@task
def load_product_dimension(input_file, **kwargs):
    """Load product dimension data from source file"""
    df = pd.read_json(input_file)
    products = df[['product_id', 'product_name', 'product_category', 'product_price']].drop_duplicates()
    output_path = os.path.join(os.path.dirname(input_file), 'product_dimension.csv')
    products.to_csv(output_path, index=False)
    return output_path

@task
def load_location_dimension(input_file, **kwargs):
    """Load location dimension data from source file"""
    df = pd.read_json(input_file)
    locations = df[['location_id', 'city', 'state', 'country', 'zip_code']].drop_duplicates()
    output_path = os.path.join(os.path.dirname(input_file), 'location_dimension.csv')
    locations.to_csv(output_path, index=False)
    return output_path

@task
def load_order_dimension(input_file, **kwargs):
    """Load order dimension data from source file"""
    df = pd.read_json(input_file)
    orders = df[['order_id', 'order_date', 'order_status', 'customer_id']].drop_duplicates()
    output_path = os.path.join(os.path.dirname(input_file), 'order_dimension.csv')
    orders.to_csv(output_path, index=False)
    return output_path

@task
def load_shipping_dimension(input_file, **kwargs):
    """Load shipping dimension data from source file"""
    df = pd.read_json(input_file)
    shipping = df[['shipping_id', 'shipping_method', 'shipping_cost', 'order_id']].drop_duplicates()
    output_path = os.path.join(os.path.dirname(input_file), 'shipping_dimension.csv')
    shipping.to_csv(output_path, index=False)
    return output_path

@task
def load_department_dimension(input_file, **kwargs):
    """Load department dimension data from source file"""
    df = pd.read_json(input_file)
    departments = df[['department_id', 'department_name', 'manager']].drop_duplicates()
    output_path = os.path.join(os.path.dirname(input_file), 'department_dimension.csv')
    departments.to_csv(output_path, index=False)
    return output_path

@task
def load_metadata_dimension(input_file, **kwargs):
    """Load metadata dimension data from source file"""
    df = pd.read_json(input_file)
    metadata = df[['metadata_id', 'created_at', 'updated_at', 'source']].drop_duplicates()
    output_path = os.path.join(os.path.dirname(input_file), 'metadata_dimension.csv')
    metadata.to_csv(output_path, index=False)
    return output_path
