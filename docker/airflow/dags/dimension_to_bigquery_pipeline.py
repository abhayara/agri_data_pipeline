from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from data_loaders.dimension_loaders import load_dimension_data
from data_exporters.bigquery_exporter import export_to_bigquery
from utils.constants import (
    DIMENSION_TABLES,
    BQ_PROJECT_ID,
    BQ_DATASET,
    BQ_LOCATION,
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
    'dimension_to_bigquery',
    default_args=default_args,
    description='Load dimension tables to BigQuery',
    schedule_interval=timedelta(days=int(PROCESSING_INTERVAL)),
    start_date=days_ago(1),
    catchup=False,
    tags=['dimensions', 'bigquery'],
)

for dimension in DIMENSION_TABLES:
    def process_dimension(dimension_name=dimension, **kwargs):
        # Load dimension data
        data = load_dimension_data(dimension_name)
        
        # Export to BigQuery
        export_to_bigquery(
            data=data,
            project_id=BQ_PROJECT_ID,
            dataset_id=BQ_DATASET,
            table_id=f"{dimension_name}_dim",
            location=BQ_LOCATION
        )
        
        return f"Processed {dimension_name} dimension table"
    
    task = PythonOperator(
        task_id=f'process_{dimension}_dimension',
        python_callable=process_dimension,
        provide_context=True,
        dag=dag,
    )
