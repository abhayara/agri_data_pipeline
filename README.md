# Agricultural Data Pipeline

This project implements a comprehensive data pipeline for agricultural data processing, using both streaming and batch processing approaches.

## Architecture Overview

The pipeline consists of the following services:

- **Kafka**: For data streaming (Broker, Zookeeper, Schema Registry, Control Center, and REST API)
- **Postgres**: For data storage and metadata
- **Airflow**: For workflow orchestration
- **Spark**: For data processing
- **Metabase**: For data visualization

## Recent Improvements

### 1. Centralized Environment Variables

- Created a central `.env` file to manage environment variables
- Updated `start_docker.sh` to check for `.env` file and load variables
- Standardized environment variable usage across all services

### 2. Robust Retry Mechanisms and Error Handling

- Implemented comprehensive retry mechanisms in Airflow DAGs
- Added `tenacity` for exponential backoff in critical operations
- Enhanced logging and error tracking
- Added graceful shutdown capability to streaming components

### 3. Health Checks for All Services

- Added health checks for all Docker services to ensure proper startup order
- Implemented dependency checks in Airflow DAGs to verify external services
- Added service dependency validation to prevent cascading failures

### 4. Improved Error Handling in Data Processing

- Enhanced error handling in Kafka producers and consumers
- Implemented batch processing with proper error recovery
- Added transaction support and validation for data integrity
- Improved logging across all components

### 5. Service Dependencies

- Used Docker Compose's `depends_on` feature with health checks
- Ensured proper service startup order
- Prevented services from starting before dependencies are ready

## Quick Start

1. Copy `.env.example` to `.env` and configure environment variables
2. Run `./docker/start_docker.sh` to start the services
3. Access services:
   - Airflow: http://localhost:8080
   - Spark Master: http://localhost:8080 (alternate port may be configured)
   - Kafka Control Center: http://localhost:9021
   - Metabase: http://localhost:3000
   - PgAdmin: http://localhost:80

## Data Flow

### Streaming Pipeline
1. Kafka producer generates synthetic agricultural data
2. Kafka consumer processes and stores data in GCS
3. Airflow DAG loads data into BigQuery

### Batch Pipeline
1. Spark reads raw data from GCS
2. Spark transforms and processes data
3. Airflow DAG loads transformed data into BigQuery

## Troubleshooting

Common issues and their solutions:

- **Port conflicts**: Check for services using the same ports
- **Kafka connection issues**: Ensure Zookeeper is properly initialized
- **Missing environment variables**: Check your `.env` file configuration
- **GCP credential errors**: Verify that `gcp-creds.json` is properly mounted 