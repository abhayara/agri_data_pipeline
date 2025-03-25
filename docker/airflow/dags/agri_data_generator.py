"""
DAG to generate agricultural data and produce it to Kafka.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_loaders.agri_kafka_producer import produce_agri_data_to_kafka

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'agri_data_generator',
    default_args=default_args,
    description='Generate and send agricultural data to Kafka',
    schedule_interval=timedelta(minutes=30),  # Run every 30 minutes
    catchup=False,
    max_active_runs=1,
)

# Create a task that produces data to Kafka
produce_data_task = PythonOperator(
    task_id='produce_agri_data_to_kafka',
    python_callable=produce_agri_data_to_kafka.function,
    op_kwargs={'num_records': 50},  # Generate 50 records per run
    dag=dag,
)

# Set task dependencies (if we had more tasks)
produce_data_task 