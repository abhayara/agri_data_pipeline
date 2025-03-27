FROM python:3.11

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
  wget \
  software-properties-common \
  lsb-release \
  gcc \
  make \
  libsasl2-modules-gssapi-mit \
  krb5-user \
  && wget -qO - https://packages.confluent.io/deb/7.0/archive.key | apt-key add - \
  && add-apt-repository "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main" \
  && apt update \
  && apt install -y librdkafka-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Copy the consumer code
COPY streaming_pipeline/consumer.py /app/
COPY streaming_pipeline/config.py /app/

# Create a properties file
RUN echo "bootstrap.servers=agri_data_pipeline-kafka:9092" > /app/consumer.properties \
    && echo "client.id=data-consumer-client" >> /app/consumer.properties \
    && echo "group.id=agri_data_consumer" >> /app/consumer.properties \
    && echo "auto.offset.reset=earliest" >> /app/consumer.properties \
    && echo "enable.auto.commit=false" >> /app/consumer.properties \
    && echo "broker.address.family=v4" >> /app/consumer.properties \
    && echo "session.timeout.ms=30000" >> /app/consumer.properties \
    && echo "heartbeat.interval.ms=10000" >> /app/consumer.properties \
    && echo "max.poll.interval.ms=300000" >> /app/consumer.properties \
    && echo "fetch.max.bytes=52428800" >> /app/consumer.properties \
    && echo "max.partition.fetch.bytes=10485760" >> /app/consumer.properties \
    && echo "debug=consumer,cgrp,topic" >> /app/consumer.properties \
    && echo "socket.nagle.disable=true" >> /app/consumer.properties \
    && echo "internal.name.resolution=true" >> /app/consumer.properties \
    && echo "resolve.canonical.bootstrap.servers.only=false" >> /app/consumer.properties

# Create directory for credentials
RUN mkdir -p /app/credentials

# Install Python dependencies
RUN pip install --no-cache-dir confluent-kafka avro fastavro pandas numpy google-cloud-storage tenacity

# Environment variables with default values
ENV KAFKA_BOOTSTRAP_SERVERS=agri_data_pipeline-kafka:9092 \
    KAFKA_TOPIC=agri_data \
    KAFKA_GROUP_ID=agri_data_consumer \
    KAFKA_AUTO_OFFSET_RESET=earliest

# Run the consumer script
CMD ["python", "consumer.py"] 