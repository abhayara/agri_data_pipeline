FROM apache/airflow:2.7.1

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    gcc \
    g++ \
    postgresql-client \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Set working directory
WORKDIR /opt/airflow

# Add DAGs
COPY ./dags /opt/airflow/dags

# Add necessary scripts and configurations
COPY ./config /opt/airflow/config
COPY ./data /opt/airflow/data

# Create directories for organization
RUN mkdir -p /opt/airflow/modules/data_loaders \
    /opt/airflow/modules/transformers \
    /opt/airflow/modules/data_exporters \
    /opt/airflow/modules/utils
