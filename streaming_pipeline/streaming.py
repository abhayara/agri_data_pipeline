from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import from_json, col
import os
from config import TOPIC

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 pyspark-shell'

credentials_location = './gcp-creds.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "./data/gcs-connector-hadoop3-2.2.5.jar ") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)\

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

gcs_bucket_path = "gs://de-zoomcamp-project-data/"

# Get Kafka bootstrap servers from environment variable or use default
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,broker:29092')

df_raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", TOPIC ) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()

# Convert the 'value' column from binary to string
df_raw_stream = df_raw_stream.withColumn("value_str", df_raw_stream["value"].cast("string"))

# Define the schema of your JSON data
json_schema = "farm_id string, farm_type string, farm_size_acres int, crop_id string, crop_type string, crop_variety string, planting_date string, harvest_date string, weather_temperature double, weather_humidity double, weather_rainfall double, weather_sunlight double, soil_type string, soil_ph double, soil_moisture double, expected_yield double, actual_yield double, yield_unit string, production_cost double, market_price double, total_revenue double, profit_margin double, sustainability_score double, timestamp string"

# Apply the transformation to create separate columns for each field
df_with_json = df_raw_stream.select("*",from_json(col("value_str"), json_schema).alias("value_json")).select("*","value_json.*").drop("value_str", "value_json")

# Selecting columns and casting their types
df_with_json = df_with_json.select(
    'farm_id', 'farm_type', 'farm_size_acres', 'crop_id', 'crop_type',
    'planting_date', 'harvest_date',
    'weather_temperature', 'weather_humidity', 'weather_rainfall',
    'soil_type', 'soil_ph', 'soil_moisture',
    'expected_yield', 'actual_yield', 'yield_unit',
    'production_cost', 'market_price', 'total_revenue', 'profit_margin',
    'sustainability_score',
    'timestamp', 'offset'
)

# def stop_query(query_name=None):
#     if query_name is None:
#         # Stop all queries
#         write_fact_query.stop()
#         write_time_query.stop()
#         # Add more queries here if needed
#     elif query_name == "write_fact_query":
#         write_fact_query.stop()
#     elif query_name == "write_time_query":
#         write_time_query.stop()
#     # Add more conditions for other query names if needed
#     else:
#         print("Invalid query name")

# # Example usage to stop a specific query
# stop_query("write_fact_query")  # Stop only the "write_fact_query" query

# # Example usage to stop all queries
# stop_query()  # Stop all queries


# Define a function to write a DataFrame to GCS
def write_to_gcs(df, batch_id, path):
    # Define the output path for this batch
    output_path = gcs_bucket_path + f"streaming_data/{path}/batch_{batch_id}/"
    # Write the batch to GCS
    df.write.format("parquet").mode("overwrite").save(output_path)

# Define your streaming query
agri_data_query = df_with_json.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_gcs(df, batch_id, 'agri_data')) \
    .start()

# Define dimension stream queries
farm_dim_query = df_with_json.select('farm_id', 'farm_type', 'farm_size_acres').writeStream \
    .foreachBatch(lambda df, batch_id: write_to_gcs(df, batch_id, 'dimensions/farm')) \
    .start()

crop_dim_query = df_with_json.select('crop_id', 'crop_type').writeStream \
    .foreachBatch(lambda df, batch_id: write_to_gcs(df, batch_id, 'dimensions/crop')) \
    .start()

# Wait for the streaming queries to finish
agri_data_query.awaitTermination()
farm_dim_query.awaitTermination()
crop_dim_query.awaitTermination()