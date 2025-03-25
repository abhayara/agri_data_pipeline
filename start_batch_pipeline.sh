#!/bin/bash

# Start the agricultural data batch pipeline
echo "Starting Agricultural Data Batch Pipeline..."

# Check if Spark is running
if ! docker ps | grep -q "spark-master"; then
    echo "Starting Spark cluster..."
    docker-compose -f docker/spark/docker-compose.yml up -d
    sleep 10  # Allow time for Spark to start
else
    echo "Spark cluster is already running"
fi

# Check if data exists in GCS bucket
echo "Checking for data in GCS..."
if [[ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
    echo "GOOGLE_APPLICATION_CREDENTIALS not set. Using default path: ./gcp-creds.json"
    export GOOGLE_APPLICATION_CREDENTIALS="./gcp-creds.json"
fi

# Run the Spark transformation pipeline
echo "Running Spark transformation pipeline..."
cd batch_pipeline/export_to_gcs
python pipeline.py
cd ../..

# Run the BigQuery export pipeline
echo "Running BigQuery export pipeline..."
cd batch_pipeline/export_to_big_query

# Export dimension tables
for dim in farm crop weather soil harvest; do
    echo "Exporting $dim dimension to BigQuery..."
    python data_exporters/${dim}_dim_to_big_query.py
done

# Export fact tables
for fact in production yield sustainability; do
    echo "Exporting $fact facts to BigQuery..."
    python data_exporters/${fact}_facts_to_big_query.py
done

cd ../..

echo "Agricultural Data Batch Pipeline completed successfully!" 