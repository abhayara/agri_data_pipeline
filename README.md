# Agricultural Data Pipeline

[![GitHub](https://img.shields.io/badge/GitHub-Agri_Data_Pipeline-blue?logo=github)](https://github.com/abhayra12/agri_data_pipeline)
[![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Streaming-red?logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-Transformations-orange?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Data_Warehouse-green?logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)

## Project Overview

This project implements a comprehensive data pipeline for agricultural data processing, using both streaming and batch processing approaches. The Agricultural Data Pipeline integrates real-time data from various agricultural sources, processes them efficiently, stores historical data for analysis, and provides visualization and analytics tools for informed decision-making in agriculture.

### Problem Statement

**Challenge:** Traditional agricultural data analysis relies on static, historical data, leading to delayed insights, inefficient resource allocation, and missed optimization opportunities across farms, crops, and sustainability metrics.

**Proposed Solution:**
1. **Integrating Data Streams:** Real-time data on farms, crops, weather, and production metrics are collected using Kafka.
2. **Real-time Data Processing:** Streaming pipelines analyze incoming data, identifying patterns and anomalies.
3. **Historical Data Storage:** Comprehensive historical data stored in BigQuery enables trend analysis and predictive modeling.
4. **Advanced Analytics:** dbt transformations create business-level metrics for sustainability, yield optimization, and farm performance.
5. **Visualization and Insights:** Metabase dashboards provide actionable insights for agricultural decision-making.

## Architecture Overview

The pipeline consists of the following services:

- **Kafka**: For data streaming (Broker, Zookeeper, Schema Registry, Control Center, and REST API)
- **Postgres**: For data storage and metadata
- **Airflow**: For workflow orchestration
- **Spark**: For data processing
- **BigQuery**: For data warehousing
- **dbt**: For data transformations and business logic
- **Metabase**: For data visualization

### Data Flow

- **Data Ingestion**: Agricultural events from sensors, farm systems, and CSV files are produced in a streaming pipeline and sent to Kafka topics.
  
- **Batch Processing**: Kafka Streaming pipeline consumes data and stores it in Google Cloud Storage as "raw_data" (bronze-level data).

- **Transformation**: Raw data from GCS undergoes transformation by Apache Spark into OLAP database design (star schema format) and is stored back into GCS as "transformed" silver-level data.

- **Export to BigQuery**: The pipeline exports transformed data from GCS to BigQuery, creating dimensional and fact tables.

- **DBT Transformation**: Transformed data in BigQuery is further processed into "business transformed" gold-level data using dbt, providing advanced agricultural analytics.

- **Terraform**: Infrastructure as Code provided by Terraform creates GCP resources like cloud storage buckets, BigQuery datasets, and compute services.

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

## Other Features

- **Data Quality Assurance**: The architecture incorporates three data levels—bronze, silver, and gold—ensuring high-quality data for analysts and scientists.

- **Flexibility and Scalability**: Dockerized containers offer flexibility and scalability, enabling seamless integration and deployment of components.

- **Comprehensive Analytics**: The gold-level data in BigQuery serves as a foundation for dashboarding, reporting, and machine learning for agricultural optimization.

## Tech Stack

- **Docker**: Containerization platform providing isolation, portability, and scalability
- **Apache Kafka**: Distributed streaming platform for real-time data ingestion
- **Apache Airflow**: Workflow orchestration tool for managing data pipelines
- **Apache Spark**: Distributed computing framework for processing large-scale data
- **dbt (Data Build Tool)**: Data transformation tool for analytics models
- **PostgreSQL**: Relational database for structured data storage
- **Metabase**: Business intelligence tool for creating visualizations and dashboards
- **Google BigQuery**: Cloud data warehouse for storing and analyzing datasets
- **Google Cloud Storage (GCS)**: Object storage for data lake implementation
- **Terraform**: Infrastructure as code tool for provisioning cloud resources

## Pipeline Overview

### 1. Streaming Pipeline

- **Folder**: [Streaming Pipeline](./streaming_pipeline/)
- Processes continuous data streams in real-time from agricultural sources
- Utilizes Kafka for efficient data ingestion, processing, and storage
- **Execution**: Detailed in the Streaming Pipeline documentation

### 2. Batch Pipeline

- **Folder**: [Batch Pipeline](./batch_pipeline/)
- **Spark Pipeline**: Processes raw agricultural data from GCS, performing OLAP transformations
- **BigQuery Export**: Loads transformed data into BigQuery as dimensional and fact tables
- **Execution**: Detailed in the Batch Pipeline documentation

### 3. dbt Transformation Pipeline

- **Folder**: [Business Transformations](./business_transformations/)
- Transforms silver-level data in BigQuery into gold-level business analytics
- Creates models for farm analytics, crop performance, sustainability metrics, yield optimization, and weather impact analysis
- **Execution**: Detailed in the dbt documentation

### 4. Dockerized Services

- **Folder**: [Docker](./docker/)
- **Kafka**: Services for data streaming
- **Airflow**: Services for workflow orchestration
- **Spark**: Services for data processing
- **Metabase**: Services for data visualization
- **PostgreSQL**: Services for data storage

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

Follow these steps to set up dbt Cloud CLI:

1. Create a dbt Cloud account at [getdbt.com](https://getdbt.com/) if you don't have one
2. Create a new project in dbt Cloud for your agri data pipeline
3. Install the dbt Cloud CLI using [official instructions](https://docs.getdbt.com/docs/cloud/cloud-cli-installation)
4. Generate a service token in dbt Cloud:
   - Go to Account Settings > Service Tokens
   - Create a new token with appropriate permissions
5. Login to dbt Cloud CLI and generate your configuration file:
   ```bash
   # Install dbt Cloud CLI
   pip install dbt-cloud
   
   # Login to dbt Cloud
   dbt-cloud login
   
   # This will create ~/.dbt/dbt_cloud.yml configuration file
   ```

The `dbt_cloud.yml` file contains your project configuration and authentication information. For more details, see the [dbt Cloud CLI documentation](https://docs.getdbt.com/docs/cloud/cloud-cli-installation).

### 5. Configure Airflow API Endpoints (Manual Step)

After starting the services:

1. Access the Airflow UI at http://localhost:8080
2. Navigate to Admin > REST API Plugins
3. Copy the API endpoint for your batch pipeline DAG
4. Update API calls in your scripts with the copied endpoint

### 6. Set Up GCP Resources with Terraform

Before starting, create a service account in GCP and get the credentials.json key.
Copy it in the appropriate directory as specified in the documentation.

```bash
terraform-start
```

This initializes terraform, gives you the overall plan, and creates the resources.
To destroy resources, use the command `terraform-destroy`.

## Step-by-Step Execution Guide

Whenever you start a new terminal, run this command to make all functions available:

```bash
source commands.sh
```

### 1. Start Streaming Pipeline

```bash
start-streaming-pipeline
```

This command initiates the streaming pipeline, which involves:

- **Start Kafka**: Launches Kafka components
- **Start Airflow**: Initiates Airflow for workflow orchestration
- **Generate Data**: Triggers data generation in Airflow
- **Stream Data**: Starts streaming agricultural data

### 2. Start Batch Pipeline

```bash
start-batch-pipeline
```

This command runs the batch pipeline:

- **Start Spark**: Launches Spark cluster
- **OLAP Transformation**: Transforms raw data into dimensional models
- **Export to BigQuery**: Loads data into BigQuery

### 3. Run dbt Transformations

```bash
run-dbt-transformations
```

This creates gold-level business transformations in BigQuery using dbt Cloud.

### 4. Create Dashboards

```bash
start-metabase
```

With Metabase running, create dashboards using the transformed data.

### 5. Automated Execution

To run the entire pipeline with a single command:

```bash
start-project
```

This runs Terraform setup, streaming pipeline, batch pipeline, and dbt transformations sequentially.

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

## Troubleshooting

Common issues and their solutions:

- **Port conflicts**: Check for services using the same ports
- **Kafka connection issues**: Ensure Zookeeper is properly initialized
- **Missing environment variables**: Check your `.env` file configuration
- **GCP credential errors**: Verify that `gcp-creds.json` is properly mounted
- **dbt errors**: See the `business_transformations/dbt_guide.md` for troubleshooting 
- **Airflow connection errors**: Verify Airflow is running with `docker ps`
- **dbt Cloud CLI issues**: Ensure your `~/.dbt/dbt_cloud.yml` file exists and contains valid credentials

## Deliverables

The project delivers:

- Integration of agricultural data streams using Kafka
- Real-time data processing and analysis using Spark and Airflow
- Historical data storage and analysis in BigQuery
- Business transformations using dbt
- Interactive dashboards for agricultural insights
- Comprehensive documentation and setup guides

## Additional Resources

- [DBT Documentation](https://docs.getdbt.com/)
- [DBT Cloud CLI Documentation](https://docs.getdbt.com/docs/cloud/cloud-cli-installation)
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Terraform Documentation](https://www.terraform.io/docs) 