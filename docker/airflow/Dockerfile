FROM apache/airflow:2.7.1

USER root

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    build-essential \
    default-libmysqlclient-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Create directories for organization
RUN mkdir -p /opt/airflow/modules/data_loaders \
    /opt/airflow/modules/transformers \
    /opt/airflow/modules/data_exporters \
    /opt/airflow/modules/utils
