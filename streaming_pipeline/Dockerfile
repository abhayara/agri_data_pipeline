FROM python:3.11

RUN apt-get update && apt-get install -y --no-install-recommends \
  wget \
  software-properties-common \
  lsb-release \
  gcc \
  make \
  libsasl2-modules-gssapi-mit \
  krb5-user\
  && wget -qO - https://packages.confluent.io/deb/7.0/archive.key | apt-key add - \
  && add-apt-repository "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main" \
  && apt update \
  && apt install -y librdkafka-dev

WORKDIR /home/streaming_pipeline
COPY . /home/streaming_pipeline

# Install required Python packages
RUN pip3 install --no-cache-dir \
    confluent-kafka \
    pyarrow \
    google-cloud-storage \
    uuid

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command - override in docker-compose
CMD ["python", "data_producer.py"]