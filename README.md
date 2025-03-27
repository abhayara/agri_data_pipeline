# Agricultural Data Pipeline

A simplified data pipeline for processing agricultural data, using both streaming and batch processing approaches.

## Overview

This project implements a data pipeline for agricultural data processing that:

1. Generates synthetic agricultural data
2. Streams the data through Kafka
3. Stores raw data in Google Cloud Storage
4. Processes data using PySpark for ETL operations
5. Loads transformed data into BigQuery for analysis

## Architecture

![Architecture Diagram](images/architecture.png)

### Components:

1. **Streaming Pipeline**
   - **Producer**: Generates synthetic agricultural data (farm, crop, weather, etc.)
   - **Consumer**: Consumes data from Kafka and stores in GCS

2. **Batch Pipeline**
   - **Spark ETL**: Transforms raw data into dimension and fact tables
   - **BigQuery Loader**: Loads transformed data into BigQuery

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- GCP Account (for GCS and BigQuery)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/agri_data_pipeline.git
   cd agri_data_pipeline
   ```

2. Create environment file:
   ```bash
   cp .env.example .env
   ```

3. Update the `.env` file with your GCP credentials and other configurations.

4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Pipeline

#### Start Streaming Pipeline

```bash
# Start the producer to generate data
python streaming_pipeline/producer.py

# Start the consumer to store data in GCS
python streaming_pipeline/consumer.py
```

#### Run Batch Pipeline

```bash
# Run the ETL pipeline
python batch_pipeline/export_to_gcs/pipeline.py
```

## Data Model

The data model includes these main entities:

- **Farm Dimension**: Information about farms
- **Crop Dimension**: Information about crops
- **Weather Dimension**: Weather conditions
- **Soil Dimension**: Soil characteristics
- **Yield Facts**: Crop yield metrics
- **Sustainability Facts**: Environmental metrics

## File Structure

```
agri_data_pipeline/
├── streaming_pipeline/        # Kafka streaming components
│   ├── producer.py            # Data generator and Kafka producer
│   ├── consumer.py            # Kafka consumer to GCS
│   └── config.py              # Streaming pipeline configuration
│
├── batch_pipeline/            # Batch processing components
│   ├── export_to_gcs/         # PySpark ETL pipeline
│   │   ├── pipeline.py        # Main ETL pipeline
│   │   ├── config.py          # Pipeline configuration
│   │   └── utils.py           # Utility functions
│   │
│   └── export_to_big_query/   # BigQuery loading components
│
├── docker/                    # Docker configuration
├── .env.example               # Environment variables template
└── requirements.txt           # Python dependencies
```

## Troubleshooting

- **Connection Issues**: Ensure all services are running correctly and check the `.env` file for proper configuration
- **Version Compatibility**: The project uses compatible versions of all dependencies to avoid conflicts
- **GCP Authentication**: Make sure you have valid GCP credentials and permissions

## Dependencies

- PySpark 3.1.3
- Confluent Kafka 2.0.2
- Google Cloud Storage 2.7.0
- Google Cloud BigQuery 3.3.5
- Pandas 1.5.3
- Python-dotenv 0.21.1 