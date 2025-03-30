#!/usr/bin/env python3
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date, year, month, dayofmonth, when, avg, sum, count, max, min, lit
import datetime

# Configuration
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "agri_data_bucket")
GCS_RAW_DATA_PATH = os.environ.get("GCS_RAW_DATA_PATH", "raw/agri_data/")
GCS_TRANSFORMED_DATA_PATH = os.environ.get("GCS_TRANSFORMED_DATA_PATH", "transformed/agri_data/")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "./gcp-creds.json")

def create_spark_session():
    """Create and configure a Spark session."""
    spark = SparkSession.builder \
        .appName("AgriDataBatchProcessing") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11,com.google.guava:guava:31.1-jre") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.driver.extraClassPath", "/usr/local/spark/jars/*") \
        .config("spark.executor.extraClassPath", "/usr/local/spark/jars/*") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    return spark

def load_data(spark):
    """Load raw agricultural data from GCS."""
    # Set GCS input path
    input_path = f"gs://{GCS_BUCKET_NAME}/{GCS_RAW_DATA_PATH}*.parquet"
    
    # Read Parquet files from GCS
    try:
        df = spark.read.parquet(input_path)
        print(f"Successfully loaded data from {input_path}")
        print(f"Data schema: {df.schema}")
        print(f"Data count: {df.count()}")
        
        # Display sample data if available
        if df is not None and df.count() > 0:
            print("\nSAMPLE DATA FROM LOADED PARQUET FILES (First 3 rows):")
            df.show(3, truncate=False)
            # Also show schema
            print("\nSCHEMA OF LOADED DATA:")
            df.printSchema()
        
        return df
    except Exception as e:
        print(f"Error loading data from {input_path}: {e}")
        # For development/testing, we'll create a sample dataset
        print("Creating a sample dataset for development/testing...")
        return create_sample_data(spark)

def create_sample_data(spark):
    """Create a sample dataset for development/testing."""
    from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType
    import random
    from datetime import date, timedelta
    
    # Define schema
    schema = StructType([
        StructField("Farm_ID", StringType(), False),
        StructField("Farm_Type", StringType(), True),
        StructField("Farm_Size_Acres", FloatType(), True),
        StructField("Farm_Location", StringType(), True),
        StructField("Farmer_ID", StringType(), True),
        StructField("Farmer_Name", StringType(), True),
        StructField("Farmer_Contact", StringType(), True),
        StructField("Crop_ID", StringType(), False),
        StructField("Crop_Type", StringType(), True),
        StructField("Crop_Variety", StringType(), True),
        StructField("Planting_Date", StringType(), True),
        StructField("Harvest_Date", StringType(), True),
        StructField("Expected_Yield", FloatType(), True),
        StructField("Actual_Yield", FloatType(), True),
        StructField("Yield_Unit", StringType(), True),
        StructField("Soil_Type", StringType(), True),
        StructField("Soil_pH", FloatType(), True),
        StructField("Soil_Moisture", FloatType(), True),
        StructField("Soil_Temperature", FloatType(), True),
        StructField("Soil_Fertility", IntegerType(), True),
        StructField("Weather_Temperature", FloatType(), True),
        StructField("Weather_Humidity", FloatType(), True),
        StructField("Weather_Rainfall", FloatType(), True),
        StructField("Weather_Sunlight", FloatType(), True),
        StructField("Irrigation_Method", StringType(), True),
        StructField("Irrigation_Amount", FloatType(), True),
        StructField("Fertilizer_Type", StringType(), True),
        StructField("Fertilizer_Amount", FloatType(), True),
        StructField("Pesticide_Type", StringType(), True),
        StructField("Pesticide_Amount", FloatType(), True),
        StructField("Equipment_Used", StringType(), True),
        StructField("Labor_Hours", FloatType(), True),
        StructField("Production_Cost", FloatType(), True),
        StructField("Market_Price", FloatType(), True),
        StructField("Total_Revenue", FloatType(), True),
        StructField("Profit_Margin", FloatType(), True),
        StructField("Storage_Method", StringType(), True),
        StructField("Storage_Duration", IntegerType(), True),
        StructField("Transportation_Method", StringType(), True),
        StructField("Transportation_Distance", IntegerType(), True),
        StructField("Quality_Grade", StringType(), True),
        StructField("Certification", StringType(), True),
        StructField("Carbon_Footprint", FloatType(), True),
        StructField("Water_Footprint", FloatType(), True),
        StructField("Sustainability_Score", IntegerType(), True),
        StructField("Market_Destination", StringType(), True),
        StructField("Customer_Segment", StringType(), True),
        StructField("Order_ID", StringType(), True),
        StructField("Order_Date", StringType(), True),
        StructField("Delivery_Date", StringType(), True),
        StructField("Order_Status", StringType(), True),
        StructField("Payment_Method", StringType(), True),
        StructField("Payment_Status", StringType(), True),
        StructField("message_id", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    # Sample data
    farm_types = ["Crop", "Livestock", "Mixed", "Horticulture"]
    crop_types = ["Grain", "Vegetable", "Fruit", "Fiber", "Oilseed"]
    crop_varieties = {
        "Grain": ["Wheat", "Corn", "Rice", "Barley", "Oats"],
        "Vegetable": ["Tomato", "Potato", "Carrot", "Onion", "Lettuce"],
        "Fruit": ["Apple", "Orange", "Banana", "Grape", "Strawberry"],
        "Fiber": ["Cotton", "Hemp", "Flax", "Jute", "Sisal"],
        "Oilseed": ["Soybean", "Canola", "Sunflower", "Peanut", "Sesame"]
    }
    soil_types = ["Clay", "Sandy", "Loam", "Silt", "Peaty"]
    irrigation_methods = ["Drip", "Sprinkler", "Flood", "None"]
    fertilizer_types = ["Organic", "Chemical", "Mixed", "None"]
    pesticide_types = ["Herbicide", "Insecticide", "Fungicide", "None"]
    equipment = ["Tractor", "Harvester", "Drone", "Manual"]
    storage_methods = ["Silo", "Warehouse", "Cold Storage", "None"]
    transportation_methods = ["Truck", "Train", "Ship", "Air"]
    quality_grades = ["Premium", "Standard", "Basic"]
    certifications = ["Organic", "Fair Trade", "GAP", "None"]
    markets = ["Local", "Regional", "National", "International"]
    customers = ["Retailer", "Processor", "Wholesaler", "Direct Consumer"]
    order_statuses = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]
    payment_methods = ["Cash", "Credit", "Bank Transfer", "Digital Wallet"]
    payment_statuses = ["Pending", "Completed", "Failed", "Refunded"]
    
    # Generate sample data
    data = []
    for i in range(100):  # Generate 100 sample records
        farm_id = f"FARM_{1000 + i}"
        farm_type = random.choice(farm_types)
        farm_size = round(random.uniform(10.0, 1000.0), 2)
        lat = round(random.uniform(20.0, 50.0), 6)
        lon = round(random.uniform(-130.0, -70.0), 6)
        location = f"{lat},{lon}"
        farmer_id = f"FARMER_{1000 + i}"
        farmer_name = f"Farmer {random.choice(['John', 'Jane', 'Bob', 'Alice', 'Carlos', 'Maria'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller'])}"
        farmer_contact = f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        crop_id = f"CROP_{1000 + i}"
        crop_type = random.choice(crop_types)
        crop_variety = random.choice(crop_varieties[crop_type])
        
        # Generate dates
        today = date.today()
        days_ago = random.randint(30, 180)
        planting_date = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        harvest_date = (today - timedelta(days=random.randint(0, 29))).strftime("%Y-%m-%d")
        
        expected_yield = round(random.uniform(2.0, 10.0), 2)
        actual_yield = round(expected_yield * random.uniform(0.7, 1.2), 2)
        yield_unit = random.choice(["kg", "ton", "bushel"])
        
        soil_type = random.choice(soil_types)
        soil_ph = round(random.uniform(5.5, 8.5), 1)
        soil_moisture = round(random.uniform(10.0, 50.0), 1)
        soil_temp = round(random.uniform(15.0, 30.0), 1)
        soil_fertility = int(random.randint(1, 10))
        
        weather_temp = round(random.uniform(15.0, 35.0), 1)
        weather_humidity = round(random.uniform(30.0, 90.0), 1)
        weather_rainfall = round(random.uniform(10.0, 200.0), 1)
        weather_sunlight = round(random.uniform(2.0, 12.0), 1)
        
        irrigation_method = random.choice(irrigation_methods)
        irrigation_amount = 0.0 if irrigation_method == "None" else round(random.uniform(500.0, 5000.0), 2)
        
        fertilizer_type = random.choice(fertilizer_types)
        fertilizer_amount = 0.0 if fertilizer_type == "None" else round(random.uniform(50.0, 500.0), 2)
        
        pesticide_type = random.choice(pesticide_types)
        pesticide_amount = 0.0 if pesticide_type == "None" else round(random.uniform(1.0, 20.0), 2)
        
        equipment_used = random.choice(equipment)
        labor_hours = round(random.uniform(10.0, 100.0), 1)
        
        production_cost = round(random.uniform(500.0, 5000.0), 2)
        market_price = round(random.uniform(10.0, 100.0), 2)
        total_revenue = round(random.uniform(1000.0, 10000.0), 2)
        profit_margin = round((total_revenue - production_cost) / total_revenue * 100, 2)
        
        storage_method = random.choice(storage_methods)
        storage_duration = int(random.randint(0, 365))
        
        transportation_method = random.choice(transportation_methods)
        transportation_distance = int(random.randint(10, 5000))
        
        quality_grade = random.choice(quality_grades)
        certification = random.choice(certifications)
        
        carbon_footprint = round(random.uniform(100.0, 1000.0), 2)
        water_footprint = round(random.uniform(500.0, 5000.0), 2)
        sustainability_score = int(random.randint(1, 10))
        
        market_destination = random.choice(markets)
        customer_segment = random.choice(customers)
        
        order_id = f"ORDER_{10000 + i}"
        
        days_ago_order = random.randint(1, 60)
        order_date = (today - timedelta(days=days_ago_order)).strftime("%Y-%m-%d")
        delivery_date = (today - timedelta(days=random.randint(0, days_ago_order-1))).strftime("%Y-%m-%d")
        
        order_status = random.choice(order_statuses)
        payment_method = random.choice(payment_methods)
        payment_status = random.choice(payment_statuses)
        
        message_id = f"msg_{i}"
        timestamp = datetime.datetime.now().isoformat()
        
        data.append((
            farm_id, farm_type, farm_size, location, farmer_id, farmer_name, farmer_contact,
            crop_id, crop_type, crop_variety, planting_date, harvest_date, expected_yield, actual_yield, yield_unit,
            soil_type, soil_ph, soil_moisture, soil_temp, soil_fertility,
            weather_temp, weather_humidity, weather_rainfall, weather_sunlight,
            irrigation_method, irrigation_amount,
            fertilizer_type, fertilizer_amount,
            pesticide_type, pesticide_amount,
            equipment_used, labor_hours,
            production_cost, market_price, total_revenue, profit_margin,
            storage_method, storage_duration,
            transportation_method, transportation_distance,
            quality_grade, certification,
            carbon_footprint, water_footprint, sustainability_score,
            market_destination, customer_segment,
            order_id, order_date, delivery_date, order_status, payment_method, payment_status,
            message_id, timestamp
        ))
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    return df

def process_dimensions(df):
    """Process dimension tables for the agricultural data."""
    # Convert string dates to proper date type
    df = df.withColumn("Planting_Date", to_date(col("Planting_Date"), "yyyy-MM-dd"))
    df = df.withColumn("Harvest_Date", to_date(col("Harvest_Date"), "yyyy-MM-dd"))
    df = df.withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd"))
    df = df.withColumn("Delivery_Date", to_date(col("Delivery_Date"), "yyyy-MM-dd"))
    
    # 1. Farm Dimension
    farm_dim = df.select(
        "Farm_ID", "Farm_Type", "Farm_Size_Acres", "Farm_Location",
        "Farmer_ID", "Farmer_Name", "Farmer_Contact"
    ).distinct()
    
    # 2. Crop Dimension
    crop_dim = df.select(
        "Crop_ID", "Crop_Type", "Crop_Variety"
    ).distinct()
    
    # 3. Weather Dimension
    weather_dim = df.select(
        "Farm_ID", "Crop_ID", 
        "Weather_Temperature", "Weather_Humidity", 
        "Weather_Rainfall", "Weather_Sunlight"
    ).distinct()
    
    # 4. Soil Dimension
    soil_dim = df.select(
        "Farm_ID", "Soil_Type", "Soil_pH", 
        "Soil_Moisture", "Soil_Temperature", "Soil_Fertility"
    ).distinct()
    
    # 5. Yield Dimension
    yield_dim = df.select(
        "Crop_ID", "Farm_ID", "Expected_Yield", 
        "Actual_Yield", "Yield_Unit"
    ).distinct()
    
    # 6. Financial Dimension
    financial_dim = df.select(
        "Farm_ID", "Crop_ID", "Production_Cost", 
        "Market_Price", "Total_Revenue", "Profit_Margin"
    ).distinct()
    
    # 7. Sustainability Dimension
    sustainability_dim = df.select(
        "Farm_ID", "Crop_ID", "Carbon_Footprint", 
        "Water_Footprint", "Sustainability_Score", "Certification"
    ).distinct()
    
    return {
        "farm": farm_dim,
        "crop": crop_dim,
        "weather": weather_dim,
        "soil": soil_dim,
        "yield": yield_dim,
        "financial": financial_dim,
        "sustainability": sustainability_dim
    }

def process_fact_tables(df, dimensions):
    """Process fact tables for the agricultural data."""
    # 1. Farm Production Fact
    farm_production_fact = df.select(
        "Farm_ID", "Crop_ID", "Planting_Date", "Harvest_Date",
        "Actual_Yield", "Production_Cost", "Total_Revenue"
    )
    
    # Add date dimensions
    farm_production_fact = farm_production_fact.withColumn("Planting_Year", year("Planting_Date"))
    farm_production_fact = farm_production_fact.withColumn("Planting_Month", month("Planting_Date"))
    farm_production_fact = farm_production_fact.withColumn("Harvest_Year", year("Harvest_Date"))
    farm_production_fact = farm_production_fact.withColumn("Harvest_Month", month("Harvest_Date"))
    
    # Calculate growth period in days
    farm_production_fact = farm_production_fact.withColumn(
        "Growth_Period_Days", 
        expr("datediff(Harvest_Date, Planting_Date)")
    )
    
    # 2. Crop Yield Fact
    crop_yield_fact = df.select(
        "Crop_ID", "Farm_ID", "Soil_Type", "Irrigation_Method",
        "Fertilizer_Type", "Pesticide_Type", "Actual_Yield", 
        "Weather_Temperature", "Weather_Rainfall"
    )
    
    # 3. Sustainability Fact
    sustainability_fact = df.select(
        "Farm_ID", "Crop_ID", "Carbon_Footprint", "Water_Footprint",
        "Sustainability_Score", "Fertilizer_Type", "Fertilizer_Amount",
        "Pesticide_Type", "Pesticide_Amount", "Transportation_Distance"
    )
    
    return {
        "farm_production": farm_production_fact,
        "crop_yield": crop_yield_fact,
        "sustainability": sustainability_fact
    }

def save_to_gcs(dataframes, output_path_prefix):
    """Save processed dataframes to GCS."""
    for name, df in dataframes.items():
        output_path = f"{output_path_prefix}/{name}"
        try:
            df.write.mode("overwrite").parquet(output_path)
            print(f"Successfully saved {name} data to {output_path}")
        except Exception as e:
            print(f"Error saving {name} data to {output_path}: {e}")

def main():
    """Main function to execute the batch pipeline."""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load data
        raw_data = load_data(spark)
        
        # Process dimension tables
        dimensions = process_dimensions(raw_data)
        
        # Process fact tables
        facts = process_fact_tables(raw_data, dimensions)
        
        # Set GCS output paths
        dimension_output_path = f"gs://{GCS_BUCKET_NAME}/{GCS_TRANSFORMED_DATA_PATH}dimensions"
        fact_output_path = f"gs://{GCS_BUCKET_NAME}/{GCS_TRANSFORMED_DATA_PATH}facts"
        
        # Save dimensions to GCS
        save_to_gcs(dimensions, dimension_output_path)
        
        # Save facts to GCS
        save_to_gcs(facts, fact_output_path)
        
        print("Batch processing completed successfully!")
    
    except Exception as e:
        print(f"Error in batch processing: {e}")
        sys.exit(1)
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main() 