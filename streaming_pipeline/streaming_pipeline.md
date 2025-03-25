# Agricultural Data Streaming Pipeline Overview

The Agricultural Data Streaming Pipeline is a dynamic component designed to process continuous streams of agricultural data in real-time. Leveraging Kafka Streaming and Airflow, this pipeline efficiently handles data ingestion, processing, and storage.

## Architecture

### Data Flow
Data is generated from our agricultural data producer, which creates synthetic farm, crop, weather, and soil metrics. This data is fed into a Docker container running a streaming service. The streaming service then publishes the data to the Kafka message broker's topic named "agri_data."

### ![Streaming Pipeline Architecture](../images/Kafka_to_gcs.png)

## Airflow Pipeline Integration

In the Airflow Pipeline integration, a Kafka consumer DAG consumes data from the Kafka topic. Subsequently, the data is streamed into a GCS bucket in the form of raw parquet files. The pipeline also processes dimension tables for the agricultural data model, including:

- Farm dimension
- Crop dimension
- Weather dimension
- Soil dimension
- Harvest dimension

## Pipeline Execution

### Steps to Start the Pipeline
To initiate the Agricultural Data Streaming Pipeline, the following steps are required:

1. **Start Kafka**: Begin by starting Kafka with the command `start-kafka`. This command launches Kafka within a Docker container, exposing it at port kafka:29092.

2. **Start Airflow**: Launch the Airflow services with the command `start-airflow`. This will start the Airflow webserver, scheduler, and related services.

3. **Generate Agricultural Data**: Activate the data generation process by triggering the `agri_data_generator` DAG in Airflow, then start the streaming container with the command `stream-data`.

### Automated Execution
For streamlined execution, all these steps can be combined into a single command: `start-streaming-pipeline`.

## Data Schema

The Agricultural Data Pipeline processes data with the following schema:

- **Farm Details**: farm_id, farm_type, farm_size_acres
- **Crop Details**: crop_id, crop_type, crop_variety, planting_date, harvest_date
- **Weather Metrics**: temperature, humidity, rainfall, sunlight
- **Soil Properties**: soil_type, pH, moisture
- **Yield Information**: expected_yield, actual_yield, yield_unit
- **Financial Metrics**: production_cost, market_price, total_revenue, profit_margin
- **Sustainability**: sustainability_score

## Summary
The Agricultural Data Streaming Pipeline enables the seamless processing of continuous farm data streams. By efficiently handling data ingestion, transformation, and storage, this pipeline facilitates real-time analytics and insights generation for agricultural operations. With a clear architecture and automated execution capabilities, the pipeline serves as a cornerstone for data-driven decision-making in agriculture. For debugging errors check the file: [Debug](../debug.md).