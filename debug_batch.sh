#!/bin/bash
set -e

echo "Running batch processing in debug mode..."

# Kill any existing batch processing jobs
docker exec agri_data_pipeline-spark-master pkill -f batch_processing.py || true

# Create a debug file to capture output
docker exec agri_data_pipeline-spark-master touch /opt/spark-apps/debug_output.log

# Run the batch processing job with output captured
docker exec agri_data_pipeline-spark-master /usr/bin/spark-3.3.1-bin-hadoop3/bin/spark-submit \
  --verbose \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.executor.cores=2 \
  --conf spark.executor.instances=1 \
  --conf spark.sql.shuffle.partitions=10 \
  --conf spark.sql.autoBroadcastJoinThreshold=10485760 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.streaming.unpersist=true \
  --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///usr/bin/spark-3.3.1-bin-hadoop3/conf/log4j.properties \
  --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///usr/bin/spark-3.3.1-bin-hadoop3/conf/log4j.properties \
  --jars /usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11-shaded.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-cloud-storage-2.27.1.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/guava-31.1-jre.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-api-client-2.2.0.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-http-client-1.42.3.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-oauth2-http-1.19.0.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-credentials-1.19.0.jar \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.gs.auth.service.account.enable=true \
  --conf spark.hadoop.fs.gs.auth.service.account.json.keyfile=/opt/spark-apps/gcp-creds.json \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/spark-apps/gcp-creds.json \
  --conf spark.ui.enabled=true \
  --conf spark.ui.port=4040 \
  /opt/spark-apps/batch_processing.py 2>&1 | tee /tmp/batch_debug.log

echo "DEBUG: Batch processing job completed. Check /tmp/batch_debug.log for details." 