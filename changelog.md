

### added terraform gcp infra code

### added kafka service in docker folder wirh docker-compose

### added streaming servive docker-compose

### added postgres docker-compose

### added spark cluster docker setup

### added airflow docker setup
#### added airflow dags 

### added metabase service 

### added agricultural data streaming pipeline v1.0.0
- Added project dependencies including Kafka and Faker for data generation
- Added Kafka configuration with agricultural data schema
- Added Kafka producer for agricultural data with realistic synthetic data generation
- Added Kafka consumer with agricultural data parsing and visualization
- Added documentation for agricultural data streaming pipeline
- Added agricultural data description and schema file
- Added Kafka to GCS streaming configuration for data persistence
- Added Dockerfile and streaming utilities for containerization
- Updated .gitignore to exclude extras folder and cache files

### integrated Airflow with streaming pipeline v1.1.0
- Updated Airflow Dockerfile with streaming data pipeline requirements
- Updated Airflow docker-compose with Kafka and GCS environment variables
- Updated Airflow requirements with Kafka and data processing dependencies
- Added Airflow DAG for agricultural data generation and Kafka integration
- Added Kafka producer utility for Airflow to generate and stream agricultural data
- Added common utilities for Airflow to handle Kafka, Postgres, and GCS operations
- Added comprehensive agricultural data pipeline DAG for Kafka to GCS and BigQuery integration

### updated configuration for agricultural data processing
- Updated Spark cluster configuration for agricultural data processing
- Updated Postgres configuration for agricultural data storage
- Removed deprecated database initialization script

