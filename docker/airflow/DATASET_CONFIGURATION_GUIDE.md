# Dataset Configuration Guide

This guide shows how to configure the data pipeline for a new dataset.

## 1. Environment Variables Setup

Edit the `.env` file in the `airflow` directory to configure your dataset parameters:

```
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=your_new_topic_name  # <-- Change this to your topic name
KAFKA_GROUP_ID=your_new_consumer_group  # <-- Change this
KAFKA_AUTO_OFFSET_RESET=earliest

# Google Cloud Storage Configuration
GCS_BUCKET_NAME=your_bucket_name  # <-- Change this
GCS_TEMP_BLOB_PATH=raw/your_dataset/  # <-- Change this

# BigQuery Configuration
BQ_PROJECT_ID=your-gcp-project-id  # <-- Change this
BQ_DATASET=your_dataset_name  # <-- Change this
BQ_LOCATION=US

# Dimension Tables Configuration
DIMENSION_TABLES=dimension1,dimension2,dimension3  # <-- Change these to your dimension tables

# Pipeline Configuration
PROCESSING_INTERVAL=1  # In days
```

## 2. Custom Dimension Loaders (Optional)

For custom dimension loading logic, create a file for each dimension:

```python
# airflow/dags/data_loaders/dimensions/dimension1_loader.py
from typing import Dict, List, Any

def load_dimension1_dimension() -> List[Dict[str, Any]]:
    """
    Custom loader for dimension1
    """
    # Your custom loading logic
    data = [
        {"id": 1, "name": "Item 1", "attribute1": "value1"},
        {"id": 2, "name": "Item 2", "attribute1": "value2"},
    ]
    return data
```

## 3. Data Schema Considerations

If your dataset has a different schema:

1. Make sure the Kafka messages follow a consistent JSON format
2. Consider adding schema validation in the `kafka_consumer.py` file
3. Update transformations as needed before loading to BigQuery

## 4. Testing Your Configuration

After making changes:

1. Restart your Airflow containers to load the new environment variables:
   ```
   docker-compose down && docker-compose up -d
   ```

2. Trigger a manual DAG run in the Airflow UI to test your configuration

3. Check logs for any errors

## 5. BigQuery Table Schema

The BigQuery tables will auto-detect schema by default. If you need explicit schema control:

1. Create a schema configuration file for each table
2. Modify the `export_to_bigquery` function to use your schema

## 6. Monitoring

Monitor your pipeline execution:

1. Set up email alerts in the DAG default_args
2. Create custom SLAs for critical tasks
3. Set up monitoring in the GCP console

## 7. Troubleshooting

Common issues:

- **Kafka connection errors**: Check if your Kafka topic exists and is accessible
- **GCS permissions**: Verify your service account has correct permissions
- **BigQuery dataset**: Ensure the dataset exists in the specified location
- **Data schema issues**: Check if your data conforms to the expected schema
