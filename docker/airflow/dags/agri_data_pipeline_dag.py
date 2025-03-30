"""
agri_data_pipeline_dag
DAG for the agriculture data pipeline
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'agri_data_pipeline',
    default_args=default_args,
    description='Agriculture Data Pipeline Orchestration',
    schedule_interval='@daily',
    start_date=datetime(2025, 3, 30),
    catchup=False,
    tags=['agriculture', 'data', 'pipeline'],
) as dag:
    
    # Start pipeline
    start_pipeline = EmptyOperator(
        task_id='start_pipeline',
    )
    
    # Prepare environment
    check_environment = BashOperator(
        task_id='check_environment',
        bash_command='cd /opt/airflow/project && source scripts/main.sh && check-environment',
    )
    
    # Start Kafka
    start_kafka = BashOperator(
        task_id='start_kafka',
        bash_command='cd /opt/airflow/project && source scripts/main.sh && start-kafka',
    )
    
    # Ensure broker hostname resolution
    ensure_broker_hostname = BashOperator(
        task_id='ensure_broker_hostname',
        bash_command='cd /opt/airflow/project && source scripts/main.sh && ensure_broker_hostname_resolution',
    )
    
    # Check GCS configuration
    check_gcs_config = BashOperator(
        task_id='check_gcs_config',
        bash_command='cd /opt/airflow/project && source scripts/main.sh && check_fix_gcs_config',
    )
    
    # Start Spark
    start_spark = BashOperator(
        task_id='start_spark',
        bash_command='cd /opt/airflow/project && source scripts/main.sh && start-spark',
    )
    
    # Start streaming pipeline
    start_streaming = BashOperator(
        task_id='start_streaming',
        bash_command='cd /opt/airflow/project && source scripts/main.sh && start-streaming-pipeline',
    )
    
    # Wait for data (streaming to GCS)
    wait_for_data = BashOperator(
        task_id='wait_for_data',
        bash_command='''
        cd /opt/airflow/project && source scripts/main.sh
        MAX_ATTEMPTS=30
        ATTEMPT=1
        
        echo "Checking for raw data in GCS bucket '${GCS_BUCKET_NAME}'..."
        
        while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
            echo "Check attempt $ATTEMPT of $MAX_ATTEMPTS (waiting for data in GCS)..."
            
            if gsutil ls "gs://${GCS_BUCKET_NAME}/raw/" &>/dev/null && 
               gsutil ls "gs://${GCS_BUCKET_NAME}/raw/agri_data/" &>/dev/null && 
               [ "$(gsutil ls -r "gs://${GCS_BUCKET_NAME}/raw/agri_data/" | wc -l)" -gt 0 ]; then
                echo "Raw data found in GCS bucket! Proceeding with batch processing."
                exit 0
            fi
            
            echo "No raw data found yet. Waiting 10 seconds before checking again..."
            sleep 10
            ATTEMPT=$((ATTEMPT + 1))
        done
        
        echo "Timeout reached while waiting for raw data. You may need to check the streaming pipeline."
        exit 1
        ''',
    )
    
    # Start batch processing
    start_batch_processing = BashOperator(
        task_id='start_batch_processing',
        bash_command='cd /opt/airflow/project && source scripts/main.sh && start-batch-processing',
    )
    
    # Wait for Spark job completion
    wait_for_spark = BashOperator(
        task_id='wait_for_spark',
        bash_command='''
        cd /opt/airflow/project && source scripts/main.sh
        MAX_ATTEMPTS=20
        ATTEMPT=1
        
        echo "Checking for Spark batch processing completion..."
        
        while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
            echo "Check attempt $ATTEMPT of $MAX_ATTEMPTS (waiting for Spark job)..."
            
            # Check if any data has been processed to OLAP folder
            if gsutil ls "gs://${GCS_BUCKET_NAME}/olap/" &>/dev/null && 
               [ "$(gsutil ls -r "gs://${GCS_BUCKET_NAME}/olap/" | wc -l)" -gt 0 ]; then
                echo "Spark job completed! OLAP data found in GCS bucket."
                exit 0
            fi
            
            echo "Spark processing still ongoing. Waiting 15 seconds before checking again..."
            sleep 15
            ATTEMPT=$((ATTEMPT + 1))
        done
        
        echo "Timeout reached while waiting for Spark job."
        exit 1
        ''',
    )
    
    # Run DBT transformations (using dbt Cloud instead of container)
    run_dbt_cloud = BashOperator(
        task_id='run_dbt_cloud',
        bash_command='cd /opt/airflow/project && source scripts/main.sh && run-dbt',
    )
    
    # End pipeline
    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
    )
    
    # Define task dependencies
    start_pipeline >> check_environment >> start_kafka >> ensure_broker_hostname
    ensure_broker_hostname >> check_gcs_config >> start_spark >> start_streaming
    start_streaming >> wait_for_data >> start_batch_processing
    start_batch_processing >> wait_for_spark >> run_dbt_cloud >> end_pipeline 