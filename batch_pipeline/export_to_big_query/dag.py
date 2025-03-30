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

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the export script
from export_to_big_query.gcs_to_bigquery import main as export_to_bigquery

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
    python_callable=export_to_bigquery,
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