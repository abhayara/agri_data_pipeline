# Agricultural Data Pipeline

This project implements a comprehensive data pipeline for agricultural data processing, using both streaming and batch processing approaches.

## Architecture Overview

The pipeline consists of the following services:

- **Kafka**: For data streaming (Broker, Zookeeper, Schema Registry, Control Center, and REST API)
- **Postgres**: For data storage and metadata
- **Airflow**: For workflow orchestration
- **Spark**: For data processing
- **BigQuery**: For data warehousing
- **dbt**: For data transformations and business logic
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

### 6. Business Transformations with dbt

- Implemented dbt for advanced data transformations in BigQuery
- Created staging models for all data dimensions and facts
- Developed analytical models for farm performance, crop metrics, and sustainability
- Enabled efficient data exploration and reporting

## Setup Instructions

### 1. Prerequisites

- Docker and Docker Compose
- Python 3.8+ with pip
- Google Cloud account with BigQuery enabled
- Google Cloud service account with appropriate permissions

### 2. Initial Environment Setup

```bash
# Clone the repository
git clone https://github.com/your-username/agri-data-pipeline.git
cd agri-data-pipeline

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# OR
venv\Scripts\activate     # Windows

# Install project requirements
pip install -r requirements.txt

# Install dbt CLI
pip install dbt-bigquery
```

### 3. Configure Environment Variables

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your configuration
# Set GCP_PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS, etc.
```

### 4. Set Up dbt Cloud

```bash
# Create .dbt directory
mkdir -p ~/.dbt

# Copy dbt Cloud configuration file to .dbt directory
cp dbt_cloud.yml ~/.dbt/
```

### 5. Configure Airflow API Endpoints (Manual Step)

After starting the services:

1. Access the Airflow UI at http://localhost:8080
2. Navigate to Admin > REST API Plugins
3. Copy the API endpoint for your batch pipeline DAG
4. Update API calls in your scripts with the copied endpoint

## Quick Start

1. Copy `.env.example` to `.env` and configure environment variables
2. Run `./docker/start_docker.sh` to start the services
3. Access services:
   - Airflow: http://localhost:8080 (username: airflow, password: airflow)
   - Spark Master: http://localhost:8090
   - Kafka Control Center: http://localhost:9021
   - Metabase: http://localhost:3000
   - PgAdmin: http://localhost:5050

## Data Flow

### Streaming Pipeline
1. Kafka producer generates synthetic agricultural data
2. Kafka consumer processes and stores data in GCS
3. Airflow DAG loads data into BigQuery

### Batch Pipeline
1. Spark reads raw data from GCS
2. Spark transforms and processes data
3. Airflow DAG loads transformed data into BigQuery

### Business Transformation Pipeline (dbt)
1. dbt models transform raw BigQuery tables into staging views
2. Core analytical models are created from staging views
3. Final data models power dashboards and analytics

## Running the Pipeline

```bash
# Source the commands file to make functions available
source commands.sh

# Start the entire pipeline with dbt transformations
start-project

# Or start individual components
start-kafka
start-airflow
start-spark
start-batch-pipeline

# Run dbt transformations manually
dbt run --profiles-dir business_transformations
```

## Project Structure

```
agri_data_pipeline/
├── batch_pipeline/            # Batch processing components
├── streaming_pipeline/        # Streaming data components
├── business_transformations/  # dbt models for data transformations
├── docker/                    # Docker configurations
├── terraform/                 # Infrastructure as code
├── commands.sh                # Pipeline automation commands
└── README.md                  # This documentation
```

## Troubleshooting

Common issues and their solutions:

- **Port conflicts**: Check for services using the same ports
- **Kafka connection issues**: Ensure Zookeeper is properly initialized
- **Missing environment variables**: Check your `.env` file configuration
- **GCP credential errors**: Verify that `gcp-creds.json` is properly mounted
- **dbt errors**: See the `business_transformations/dbt_guide.md` for troubleshooting 
- **Airflow connection errors**: Verify Airflow is running with `docker ps`

## Additional Resources

- [DBT Documentation](https://docs.getdbt.com/)
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs) 