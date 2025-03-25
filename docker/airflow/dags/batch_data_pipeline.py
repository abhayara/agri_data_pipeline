"""
Airflow DAG for batch processing of agricultural data.
This DAG orchestrates the transformation of raw agricultural data using Spark
and loads the transformed data to BigQuery.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import os
import subprocess
import logging
import time
import traceback
from google.cloud import storage, bigquery
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=60),
}

# Create the DAG
dag = DAG(
    'batch_data_pipeline',
    default_args=default_args,
    description='Batch processing of agricultural data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['agricultural_data', 'batch'],
)

# Start and end tasks for better control
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.ALL_DONE,  # Run even if some tasks fail
    dag=dag,
)

# Define paths for batch processing scripts
base_path = os.getenv('BATCH_SCRIPTS_PATH', '/opt/airflow/dags/scripts/batch/')
dimension_path = f"{base_path}dimensions/"
fact_path = f"{base_path}facts/"

# Function to check GCS data availability
def check_gcs_data_availability(**kwargs):
    """Check if required data is available in GCS bucket."""
    try:
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')
        raw_data_path = os.getenv('GCS_RAW_DATA_PATH', 'raw/agri_data/')
        
        # Check if raw data exists in GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # List blobs in the raw data path
        blobs = list(bucket.list_blobs(prefix=raw_data_path))
        
        if not blobs:
            logger.warning(f"No raw data files found in gs://{bucket_name}/{raw_data_path}")
            raise Exception("No raw data available for batch processing")
            
        logger.info(f"Found {len(blobs)} raw data files available for processing")
        return True
    except Exception as e:
        logger.error(f"Error checking GCS data availability: {e}")
        logger.error(traceback.format_exc())
        raise

# Add GCS data check task
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_gcs_data_availability,
    retries=5,
    retry_delay=timedelta(minutes=10),
    dag=dag,
)

# Create BigQuery dataset
create_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    dataset_id=os.getenv('GCP_DATASET_ID', 'agri_data'),
    project_id=os.getenv('GCP_PROJECT_ID', 'agri-data-454414'),
    exists_ok=True,
    dag=dag,
)

# Create Spark job for transformation with retry mechanism
spark_transform_task = SparkSubmitOperator(
    task_id='spark_transformation',
    application=os.getenv('SPARK_MAIN_APP', f"{base_path}transform_raw_data.py"),
    name='agricultural_data_transformation',
    conn_id='spark_default',
    application_args=[
        "--input", f"gs://{os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')}/{os.getenv('GCS_RAW_DATA_PATH', 'raw/agri_data/')}",
        "--output", f"gs://{os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')}/transformed/",
        "--date", "{{ ds }}"
    ],
    conf={
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        "spark.executor.cores": "2",
        "spark.driver.maxResultSize": "1g",
        "spark.executor.instances": "2",
        "spark.debug.maxToStringFields": "100"
    },
    retries=3,
    retry_delay=timedelta(minutes=5),
    verbose=True,
    dag=dag,
)

# Function to run Python scripts with error handling
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def run_python_script(script_path, **kwargs):
    """
    Run a Python script with robust error handling and output capture.
    
    Args:
        script_path: Path to the Python script
        **kwargs: Additional arguments passed to the task context
    
    Returns:
        The output of the script execution or raises an exception on failure
    """
    try:
        logger.info(f"Running script: {script_path}")
        
        # Execute the script
        process = subprocess.Popen(
            ['python', script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Capture output
        stdout, stderr = process.communicate(timeout=300)  # 5 minute timeout
        
        # Log the output
        if stdout:
            logger.info(f"Script output: {stdout}")
        
        # Check for errors
        if process.returncode != 0:
            logger.error(f"Script error: {stderr}")
            raise Exception(f"Script execution failed with return code {process.returncode}: {stderr}")
            
        # Return success
        logger.info(f"Successfully ran script: {script_path}")
        return stdout
    except subprocess.TimeoutExpired:
        logger.error(f"Script execution timed out: {script_path}")
        process.kill()  # Ensure process is terminated
        raise Exception(f"Script execution timed out after 300 seconds: {script_path}")
    except Exception as e:
        logger.error(f"Error running script {script_path}: {e}")
        logger.error(traceback.format_exc())
        raise

# Function to check if BQ table exists
def check_table_exists(project_id, dataset_id, table_id):
    """Check if a BigQuery table exists."""
    try:
        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)
        client.get_table(table_ref)
        return True
    except Exception:
        return False

# Tasks for loading dimension tables to BigQuery with error handling
dimension_scripts = os.getenv('DIMENSION_TABLES', 'farm,crop,weather,soil,harvest').split(',')
dimension_tasks = []

for table in dimension_scripts:
    script_path = f"{dimension_path}load_{table}_to_bq.py"
    
    # Create task to run the script with error handling
    task = PythonOperator(
        task_id=f'load_{table}_dimension',
        python_callable=run_python_script,
        op_kwargs={'script_path': script_path},
        retries=3,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
        dag=dag,
    )
    
    # Add to dimension tasks list
    dimension_tasks.append(task)
    
    # Set dependency with create dataset task
    create_dataset_task >> task

# Tasks for loading fact tables to BigQuery with error handling
fact_scripts = os.getenv('FACT_TABLES', 'farm_activity,yield').split(',')
fact_tasks = []

for table in fact_scripts:
    script_path = f"{fact_path}load_{table}_to_bq.py"
    
    # Create task to run the script with error handling
    task = PythonOperator(
        task_id=f'load_{table}_fact',
        python_callable=run_python_script,
        op_kwargs={'script_path': script_path},
        retries=3,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
        dag=dag,
    )
    
    # Add to fact tasks list
    fact_tasks.append(task)
    
    # Set dependencies - fact tables should load after dimension tables
    for dim_task in dimension_tasks:
        dim_task >> task

# Set overall task dependencies
start_task >> check_data_task >> spark_transform_task >> create_dataset_task
for fact_task in fact_tasks:
    fact_task >> end_task 