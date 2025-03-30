# Agricultural Data Pipeline

This project implements an end-to-end agricultural data pipeline that processes streaming and batch data for analysis and visualization.

## Project Overview

The pipeline has the following components:

1. **Data Generation** - Synthetic agricultural data generation with fields including farm information, crop data, weather metrics, soil properties, and more.
2. **Streaming Pipeline** - Real-time data processing using Kafka and Airflow.
3. **Batch Processing** - OLAP transformations using Apache Spark.
4. **Data Warehouse Integration** - Loading processed data to BigQuery using Airflow DAGs.
5. **Analytics** - Transformation of data using dbt for business intelligence.
6. **Visualization** - Dashboard creation using Metabase.

## Architecture

- **Streaming Layer**: Kafka, Confluent, Zookeeper
- **Data Processing**: Apache Spark
- **Orchestration**: Apache Airflow
- **Storage**: Google Cloud Storage (GCS)
- **Data Warehouse**: Google BigQuery
- **Transformation**: dbt (data build tool)
- **Visualization**: Metabase

## Setup

### Prerequisites

- Docker and Docker Compose
- Google Cloud Platform account and project
- Python 3.8+ with pip
- Java JDK 11
- Network connectivity to download Docker images
- At least 8GB of RAM available for Docker

### Step-by-Step Setup Instructions

1. Clone the repository:
   ```
   git clone <repository-url>
   cd agri_data_pipeline
   ```

2. Install required Python packages:
   ```
   pip install -r requirements.txt
   ```

3. Prepare GCP credentials:
   - Create a service account in GCP with the following permissions:
     - BigQuery Admin
     - Storage Admin
     - Dataflow Admin
   - Download the JSON key file and save it as `gcp-creds.json` in the project root

4. Configure environment variables:
   ```
   cp .env.example .env
   ```
   Edit the `.env` file to update:
   - `GCP_PROJECT_ID` - Your Google Cloud project ID
   - `GCS_BUCKET_NAME` - Name of your GCS bucket (will be created if it doesn't exist)
   - Other settings as needed

5. Start the complete pipeline with a single command:
   ```
   source commands.sh && start-project
   ```
   This will:
   - Verify your environment is properly set up
   - Create GCP infrastructure with Terraform
   - Start all required Docker containers
   - Initialize the streaming and batch pipelines
   - Run dbt transformations for analytics models
   - Set up Metabase for visualization

### Alternative: Step-by-Step Manual Setup

If you prefer to start components individually for testing or development:

1. Initialize infrastructure:
   ```
   source commands.sh && terraform-start
   ```

2. Start and verify Kafka:
   ```
   source commands.sh && start-kafka && verify-kafka
   ```

3. Start Airflow:
   ```
   source commands.sh && start-airflow && verify-airflow
   ```

4. Start the streaming pipeline in foreground mode (shows logs):
   ```
   source commands.sh && docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up
   ```
   Or in background mode:
   ```
   source commands.sh && docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d
   ```

5. Start Spark for batch processing:
   ```
   source commands.sh && start-spark && verify-spark
   ```

6. Run batch processing:
   ```
   source commands.sh && start-batch-pipeline
   ```

7. Run dbt transformations:
   ```
   source commands.sh && run-dbt
   ```

8. Start Metabase for visualization:
   ```
   source commands.sh && start-metabase
   ```

## Verifying the Pipeline

After setup, you can verify all components are working properly:

```
source commands.sh && verify-all
```

This will check:
- Kafka broker and topic status
- Producer message delivery
- Consumer processing status
- Airflow setup
- Spark cluster status
- Batch pipeline processing

## Metabase Dashboard Setup

1. Access Metabase at http://localhost:3000
2. Complete the initial setup:
   - Create an admin account
   - Connect to your data source (BigQuery)
   - Enter your GCP project ID and use service account authentication

3. Creating a Dashboard:
   - Click "New" > "Dashboard"
   - Name your dashboard (e.g., "Agricultural Analytics")
   - Add questions using the "+ Add" button
   - Create visualizations for:
     - Crop yield by farm type
     - Weather impact on production
     - Soil quality metrics
     - Financial performance

4. Example queries:
   - Average yield by crop type
   - Farm production costs vs. revenue
   - Sustainability metrics over time
   - Correlation between weather and yield

## Accessing Services

- **Kafka Control Center**: http://localhost:9021
- **Airflow UI**: http://localhost:8080 (login credentials in your .env file, typically airflow/airflow)
- **Spark UI**: http://localhost:8080
- **Metabase**: http://localhost:3000

## dbt Documentation

To generate and view dbt documentation for your analytics models:

```
source commands.sh && serve-dbt-docs
```

This will make the documentation available at http://localhost:8080

## Troubleshooting

- **Verifying services**: Run `source commands.sh && verify-all`
- **Checking logs**: Run `docker logs [container_name]` (e.g., `docker logs agri_data_producer`)
- **Restarting components**:
  - Kafka: `source commands.sh && restart-kafka`
  - Streaming: `source commands.sh && restart-streaming`
  - Airflow: `source commands.sh && restart-airflow`
  - Spark: `source commands.sh && restart-spark`
  - dbt: `source commands.sh && run-dbt`
- **Complete reset**: Run `source commands.sh && full-reset-and-test`
  - The reset function will interactively ask if you want to reuse existing infrastructure resources
  - All logs will be displayed in "frontfoot" mode (immediately visible) to help troubleshoot issues
  - You can interrupt at any point and manually fix issues before continuing

## Common Issues

- If port conflicts occur, edit the port mappings in the `.env` file
- For GCP authentication issues, verify your `gcp-creds.json` has the correct permissions
- If containers fail to start, check Docker resource limits

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 