# Agricultural Data Batch Pipeline Overview

The Agricultural Data Batch Pipeline is a critical component in the data processing architecture, designed to handle large volumes of agricultural data efficiently. It consists of two main parts:

## Spark Pipeline

The Spark Pipeline is responsible for processing raw agricultural data sourced from GCS, performing necessary OLAP (Online Analytical Processing) transformations, and storing the transformed data back into GCS as Silver level data. This pipeline facilitates data refinement and preparation for downstream analytics tasks. [Pipeline Folder](./export_to_gcs/)


### Pipeline Execution
To initiate the Spark Pipeline, start Spark with the command `start-spark` and then `agri-data-transformation-pipeline`. This command kickstarts the data transformation process, leveraging the power of Spark's distributed computing capabilities.

### Architecture Overview
The Batch Pipeline architecture involves a series of data flows and transformations:

1. **Data Ingestion**: Agricultural data is sourced from farm sensors, IoT devices, and agricultural management systems, then sent to the Kafka message broker's topic `agri_data`.
  
2. **Data Consumption**: The data is consumed from Kafka by PostgreSQL database and a GCS bucket, where it is stored as raw_streaming data.

3. **Transformation**: The Spark Pipeline processes the raw data, applying OLAP transformations to derive valuable insights and prepare it for analytical purposes.

4. **Export to BigQuery**: The Airflow Pipeline exports the transformed data from GCS to BigQuery, making it accessible for analysis and reporting.

### Database Design
The transformed data is structured according to a well-defined database design for agricultural analytics, ensuring consistency, efficiency, and ease of access for analytical queries.

The data warehouse follows a star schema design with the following dimension tables:
- Farm Dimension
- Crop Dimension
- Weather Dimension
- Soil Dimension
- Harvest Dimension

And fact tables:
- Farm Production Facts
- Crop Yield Facts
- Sustainability Metrics Facts

## Airflow Pipeline

The Airflow Pipeline is responsible for exporting transformed (Silver level) data from Google Cloud Storage (GCS) and ingesting it into BigQuery. This pipeline plays a crucial role in ensuring that the processed agricultural data is readily available for analysis and reporting in BigQuery.
[Pipeline Folder](./export_to_big_query/)

### Pipeline Execution
To execute the Airflow Pipeline, run the command `docker-compose -f docker/airflow/docker-compose.yml up`. This command triggers the Airflow DAGs, instructing them to initiate the pipeline execution according to their schedule.

### Data Lineage
The agricultural data follows a clear lineage from raw data to analytics:

1. **Raw Data Collection**: Agricultural data from various sources
2. **Streaming Ingestion**: Kafka streaming pipeline processes real-time data
3. **Batch Processing**: Spark transforms and enriches the data
4. **Data Warehousing**: Airflow loads data into BigQuery
5. **Analytics**: Data is available for BI tools and dashboards

### Automated Execution
For streamlined execution, all these steps can be combined into a single command: `start-batch-pipeline`.

## Summary
The Agricultural Data Batch Pipeline plays a crucial role in the data processing workflow, enabling the efficient transformation and storage of data for agricultural analytics. By leveraging Airflow and Spark pipelines, organizations can streamline their data processing tasks and derive valuable insights from large datasets of agricultural information stored in GCS and BigQuery.