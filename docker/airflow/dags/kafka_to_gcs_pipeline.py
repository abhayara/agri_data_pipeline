from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from data_loaders.kafka_consumer import consume_from_kafka
from data_exporters.gcs_exporter import upload_to_gcs
from utils.constants import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
    GCS_BUCKET_NAME,
    GCS_TEMP_BLOB_PATH,
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
    f'{KAFKA_TOPIC}_to_gcs_pipeline',
    default_args=default_args,
    description=f'Pipeline to extract data from Kafka and load to GCS for {KAFKA_TOPIC}',
    schedule_interval=timedelta(days=int(PROCESSING_INTERVAL)),
    start_date=days_ago(1),
    catchup=False,
    tags=[KAFKA_TOPIC, 'kafka', 'gcs'],
)

def extract_and_load(**kwargs):
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

extract_load_task = PythonOperator(
    task_id='extract_and_load',
    python_callable=extract_and_load,
    provide_context=True,
    dag=dag,
)

extract_load_task
