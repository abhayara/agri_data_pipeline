from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from data_loaders.kafka_consumer import consume_from_kafka
from data_exporters.gcs_exporter import upload_to_gcs
from data_loaders.dimension_loaders import load_dimension_data
from data_exporters.bigquery_exporter import export_to_bigquery

from utils.constants import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
    GCS_BUCKET_NAME,
    GCS_TEMP_BLOB_PATH,
    BQ_PROJECT_ID,
    BQ_DATASET,
    BQ_LOCATION,
    DIMENSION_TABLES,
    PROCESSING_INTERVAL
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    f'{KAFKA_TOPIC}_complete_pipeline',
    default_args=default_args,
    description=f'Complete data pipeline for {KAFKA_TOPIC}',
    schedule_interval=timedelta(days=int(PROCESSING_INTERVAL)),
    start_date=days_ago(1),
    catchup=False,
    tags=[KAFKA_TOPIC, 'complete', 'pipeline'],
)

# Start task
start = DummyOperator(
    task_id='start',
    dag=dag
)

# Extract from Kafka and store in GCS
def extract_to_gcs(**kwargs):
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Extract data from Kafka
    data = consume_from_kafka(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET
    )
    
    # Upload data to GCS
    gcs_uri = upload_to_gcs(
        data=data,
        bucket_name=GCS_BUCKET_NAME,
        blob_path=f"{GCS_TEMP_BLOB_PATH}{date_str}/{KAFKA_TOPIC}_data.json"
    )
    
    return gcs_uri

extract_task = PythonOperator(
    task_id='extract_to_gcs',
    python_callable=extract_to_gcs,
    provide_context=True,
    dag=dag,
)

# Process dimension tables
dimension_tasks = []
for dimension in DIMENSION_TABLES:
    def process_dimension(dimension_name=dimension, **kwargs):
        # Load dimension data
        data = load_dimension_data(dimension_name)
        
        # Export to BigQuery
        table_ref = export_to_bigquery(
            data=data,
            project_id=BQ_PROJECT_ID,
            dataset_id=BQ_DATASET,
            table_id=f"{dimension_name}_dim",
            location=BQ_LOCATION
        )
        
        return table_ref
    
    dimension_task = PythonOperator(
        task_id=f'process_{dimension}_dimension',
        python_callable=process_dimension,
        provide_context=True,
        dag=dag,
    )
    dimension_tasks.append(dimension_task)

# Export raw data to BigQuery
def export_raw_to_bigquery(**kwargs):
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Assume data was already saved to GCS in extract_to_gcs task
    # Here we would process and export it to BigQuery
    
    # In a real scenario, you would:
    # 1. Read data from GCS
    # 2. Process it if needed
    # 3. Export to BigQuery
    
    # For this example, we'll mock this process
    return f"{BQ_PROJECT_ID}.{BQ_DATASET}.{KAFKA_TOPIC}_raw"

export_task = PythonOperator(
    task_id='export_raw_to_bigquery',
    python_callable=export_raw_to_bigquery,
    provide_context=True,
    dag=dag,
)

# End task
end = DummyOperator(
    task_id='end',
    dag=dag
)

# Set dependencies
start >> extract_task >> export_task
for dimension_task in dimension_tasks:
    start >> dimension_task >> end
export_task >> end
