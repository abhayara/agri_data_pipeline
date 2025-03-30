import os
from dotenv import load_dotenv
import pyspark.sql.types as T

# Load environment variables
load_dotenv()

# Kafka Configuration
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'agri_data')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'agri_data_consumer')

# GCS Configuration
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')
GCS_RAW_PATH = os.getenv('GCS_TEMP_BLOB_PATH', 'raw/agri_data/')

# Agriculture Data Schema
AGRI_SCHEMA = T.StructType([
    T.StructField("farm_id", T.StringType()),
    T.StructField("farm_name", T.StringType()),
    T.StructField("farm_location", T.StringType()),
    T.StructField("farm_size_acres", T.DoubleType()),
    T.StructField("crop_id", T.StringType()),
    T.StructField("crop_type", T.StringType()),
    T.StructField("planting_date", T.StringType()),
    T.StructField("harvest_date", T.StringType()),
    T.StructField("weather_temperature", T.DoubleType()),
    T.StructField("weather_humidity", T.DoubleType()),
    T.StructField("weather_rainfall", T.DoubleType()),
    T.StructField("soil_type", T.StringType()),
    T.StructField("soil_ph", T.DoubleType()),
    T.StructField("expected_yield", T.DoubleType()),
    T.StructField("actual_yield", T.DoubleType()),
    T.StructField("yield_unit", T.StringType()),
    T.StructField("sustainability_score", T.DoubleType()),
    T.StructField("timestamp", T.TimestampType())
]) 