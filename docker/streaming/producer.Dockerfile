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

# Copy the producer code
COPY streaming_pipeline/producer.py /app/
COPY streaming_pipeline/config.py /app/

# Create a properties file
RUN echo "bootstrap.servers=agri_data_pipeline-kafka:9092" > /app/producer.properties \
    && echo "client.id=data-producer-client" >> /app/producer.properties \
    && echo "broker.address.family=v4" >> /app/producer.properties \
    && echo "message.timeout.ms=10000" >> /app/producer.properties \
    && echo "request.timeout.ms=30000" >> /app/producer.properties \
    && echo "retry.backoff.ms=500" >> /app/producer.properties \
    && echo "message.send.max.retries=5" >> /app/producer.properties \
    && echo "debug=broker,topic,msg" >> /app/producer.properties \
    && echo "socket.nagle.disable=true" >> /app/producer.properties \
    && echo "internal.name.resolution=true" >> /app/producer.properties \
    && echo "resolve.canonical.bootstrap.servers.only=false" >> /app/producer.properties

# Install Python dependencies
RUN pip install --no-cache-dir confluent-kafka avro fastavro pandas numpy faker tenacity

# Environment variables with default values
ENV KAFKA_BOOTSTRAP_SERVERS=agri_data_pipeline-kafka:9092 \
    KAFKA_TOPIC=agri_data

# Run the producer script
CMD ["python", "producer.py"] 