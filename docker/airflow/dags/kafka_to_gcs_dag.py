"""
Kafka to GCS Streaming DAG
This DAG consumes data from Kafka and stores it in GCS.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import sys
import logging

# Import path for the consumer script
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

# Import the consumer function
from data_consumer import consume_messages

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
    'kafka_to_gcs_streaming',
    default_args=default_args,
    description='Consume data from Kafka and store in GCS',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    catchup=False,
    max_active_runs=1,
)

# Define the consumer task
consume_task = PythonOperator(
    task_id='consume_from_kafka',
    python_callable=consume_messages,
    provide_context=True,
    dag=dag,
)

# Logging task to confirm execution
log_task = BashOperator(
    task_id='log_completion',
    bash_command='echo "Kafka to GCS streaming completed at $(date)"',
    dag=dag,
)

# Define task dependencies
consume_task >> log_task 