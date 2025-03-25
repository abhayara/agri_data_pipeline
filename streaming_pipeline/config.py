import pyspark.sql.types as T

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'agri_data'

AGRI_SCHEMA = T.StructType([
    T.StructField("farm_id", T.StringType()),
    T.StructField("farm_type", T.StringType()),
    T.StructField("farm_size_acres", T.IntegerType()),
    T.StructField("crop_id", T.StringType()),
    T.StructField("crop_type", T.StringType()),
    T.StructField("crop_variety", T.StringType()),
    T.StructField("planting_date", T.StringType()),
    T.StructField("harvest_date", T.StringType()),
    T.StructField("weather_temperature", T.DoubleType()),
    T.StructField("weather_humidity", T.DoubleType()),
    T.StructField("weather_rainfall", T.DoubleType()),
    T.StructField("weather_sunlight", T.DoubleType()),
    T.StructField("soil_type", T.StringType()),
    T.StructField("soil_ph", T.DoubleType()),
    T.StructField("soil_moisture", T.DoubleType()),
    T.StructField("expected_yield", T.DoubleType()),
    T.StructField("actual_yield", T.DoubleType()),
    T.StructField("yield_unit", T.StringType()),
    T.StructField("production_cost", T.DoubleType()),
    T.StructField("market_price", T.DoubleType()),
    T.StructField("total_revenue", T.DoubleType()),
    T.StructField("profit_margin", T.DoubleType()),
    T.StructField("sustainability_score", T.DoubleType()),
    T.StructField("timestamp", T.StringType())
]) 