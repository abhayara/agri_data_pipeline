"""
GCS to BigQuery Export DAG
This DAG checks for new data in GCS every 10 minutes and exports it to BigQuery.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import sys
import logging

# Add helpers directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'helpers'))

# Import the helper function
try:
    from helpers import export_gcs_to_bigquery
    IMPORT_ERROR = None
except Exception as e:
    IMPORT_ERROR = str(e)
    # Fallback function in case import fails
    def export_gcs_to_bigquery(**kwargs):
        if IMPORT_ERROR:
            raise ImportError(f"Failed to import helper: {IMPORT_ERROR}")
        return {"status": "error", "message": "Helper function not available"}

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['alerts@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'gcs_to_bigquery_export',
    default_args=default_args,
    description='Export data from GCS to BigQuery',
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    max_active_runs=1,
)

# Define the export task
export_task = PythonOperator(
    task_id='export_to_bigquery',
    python_callable=export_gcs_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Logging task to confirm execution
log_task = BashOperator(
    task_id='log_completion',
    bash_command='echo "GCS to BigQuery export completed at $(date)"',
    dag=dag,
)

# Define task dependencies
export_task >> log_task 