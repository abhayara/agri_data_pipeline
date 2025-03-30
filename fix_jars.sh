#!/bin/bash
set -e

echo "Fixing JAR files for Spark..."

# Create the download script
cat > /tmp/download_jars.sh << 'EOL'
#!/bin/bash
set -e

echo "Installing wget..."
apt-get update -y && apt-get install -y wget

echo "Downloading JAR files..."
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11.jar https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.11/gcs-connector-hadoop3-2.2.11.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11-shaded.jar https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.11/gcs-connector-hadoop3-2.2.11-shaded.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/google-cloud-storage-2.27.1.jar https://repo1.maven.org/maven2/com/google/cloud/google-cloud-storage/2.27.1/google-cloud-storage-2.27.1.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/guava-31.1-jre.jar https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/google-api-client-2.2.0.jar https://repo1.maven.org/maven2/com/google/api-client/google-api-client/2.2.0/google-api-client-2.2.0.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/google-http-client-1.42.3.jar https://repo1.maven.org/maven2/com/google/http-client/google-http-client/1.42.3/google-http-client-1.42.3.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-oauth2-http-1.19.0.jar https://repo1.maven.org/maven2/com/google/auth/google-auth-library-oauth2-http/1.19.0/google-auth-library-oauth2-http-1.19.0.jar
wget -q -O /usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-credentials-1.19.0.jar https://repo1.maven.org/maven2/com/google/auth/google-auth-library-credentials/1.19.0/google-auth-library-credentials-1.19.0.jar

echo "Downloading completed."
echo "Setting permissions..."
chmod 644 /usr/bin/spark-3.3.1-bin-hadoop3/jars/*.jar

echo "Listing downloaded files:"
ls -la /usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs* /usr/bin/spark-3.3.1-bin-hadoop3/jars/google* /usr/bin/spark-3.3.1-bin-hadoop3/jars/guava-31.1-jre.jar

echo "Download and setup of JAR files completed successfully."
EOL

# Copy the script to the container
docker cp /tmp/download_jars.sh agri_data_pipeline-spark-master:/tmp/download_jars.sh
docker exec agri_data_pipeline-spark-master chmod +x /tmp/download_jars.sh

# Execute the script in the container
echo "Executing download script in the container..."
docker exec agri_data_pipeline-spark-master /tmp/download_jars.sh

echo "JAR files have been fixed successfully."
echo "Restarting batch processing..."

# Kill any existing batch processing jobs
docker exec agri_data_pipeline-spark-master pkill -f batch_processing.py || true

# Start a new batch processing job
docker exec -d agri_data_pipeline-spark-master /usr/bin/spark-3.3.1-bin-hadoop3/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.executor.cores=2 \
  --conf spark.executor.instances=1 \
  --conf spark.sql.shuffle.partitions=10 \
  --conf spark.sql.autoBroadcastJoinThreshold=10485760 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.streaming.unpersist=true \
  --jars /usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11-shaded.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/gcs-connector-hadoop3-2.2.11.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-cloud-storage-2.27.1.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/guava-31.1-jre.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-api-client-2.2.0.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-http-client-1.42.3.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-oauth2-http-1.19.0.jar,/usr/bin/spark-3.3.1-bin-hadoop3/jars/google-auth-library-credentials-1.19.0.jar \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.gs.auth.service.account.enable=true \
  --conf spark.hadoop.fs.gs.auth.service.account.json.keyfile=/opt/spark-apps/gcp-creds.json \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/spark-apps/gcp-creds.json \
  /opt/spark-apps/batch_processing.py

echo "Batch processing job started in the background."
echo "Check the GCS status in a few minutes to see if OLAP data has been created." 