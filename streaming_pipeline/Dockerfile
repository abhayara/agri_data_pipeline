FROM python:3.11

# Install network tools for troubleshooting
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    netcat-openbsd \
    iputils-ping \
    net-tools \
    dnsutils \
    && rm -rf /var/lib/apt/lists/*

# Set up working directory
WORKDIR /home/streaming_pipeline

# Copy the source code
COPY . /home/streaming_pipeline

# Install Python dependencies
RUN pip3 install --no-cache-dir \
    confluent-kafka \
    pyarrow \
    google-cloud-storage \
    pandas \
    numpy

# Default command (will be overridden in docker-compose)
CMD ["python", "data_producer.py"]