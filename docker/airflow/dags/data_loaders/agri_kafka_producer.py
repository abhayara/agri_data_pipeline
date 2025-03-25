"""
Kafka producer for agricultural data.
This script generates synthetic agricultural data and sends it to a Kafka topic.
"""
import os
import json
import random
import time
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
from airflow.decorators import task
from pathlib import Path

# Function to get a producer connection
def get_kafka_producer():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

# Function to generate synthetic agricultural data
def generate_agri_data():
    # Farm details
    farm_id = f"FARM{random.randint(1, 100)}"
    farm_type = random.choice(["Crop", "Livestock", "Mixed", "Horticulture"])
    farm_size = random.randint(10, 1000)
    
    # Crop details
    crop_id = f"CROP{random.randint(1, 50)}"
    crop_type = random.choice(["Grain", "Vegetable", "Fruit", "Fiber", "Oilseed"])
    crop_variety = f"Variety{random.randint(1, 20)}"
    
    # Weather details
    temperature = round(random.uniform(10, 35), 1)
    humidity = round(random.uniform(30, 90), 1)
    rainfall = round(random.uniform(0, 100), 1)
    sunlight = round(random.uniform(4, 12), 1)
    
    # Soil details
    soil_type = random.choice(["Clay", "Sandy", "Loam", "Silt", "Peaty"])
    soil_ph = round(random.uniform(5.0, 8.0), 1)
    soil_moisture = round(random.uniform(10, 90), 1)
    
    # Yield details
    expected_yield = round(random.uniform(100, 500), 2)
    actual_yield = round(expected_yield * random.uniform(0.7, 1.3), 2)
    
    # Cost and revenue
    production_cost = round(random.uniform(50, 200) * farm_size / 100, 2)
    market_price = round(random.uniform(10, 50), 2)
    total_revenue = round(actual_yield * market_price, 2)
    profit_margin = round((total_revenue - production_cost) / total_revenue * 100, 2)
    
    # Dates
    current_date = datetime.now()
    planting_date = (current_date - timedelta(days=random.randint(60, 180))).strftime('%Y-%m-%d')
    harvest_date = (current_date - timedelta(days=random.randint(10, 30))).strftime('%Y-%m-%d')
    
    # Create a data record
    data = {
        "farm_id": farm_id,
        "farm_type": farm_type,
        "farm_size_acres": farm_size,
        "crop_id": crop_id,
        "crop_type": crop_type,
        "crop_variety": crop_variety,
        "planting_date": planting_date,
        "harvest_date": harvest_date,
        "weather_temperature": temperature,
        "weather_humidity": humidity,
        "weather_rainfall": rainfall,
        "weather_sunlight": sunlight,
        "soil_type": soil_type,
        "soil_ph": soil_ph,
        "soil_moisture": soil_moisture,
        "expected_yield": expected_yield,
        "actual_yield": actual_yield,
        "yield_unit": random.choice(["kg", "ton", "bushel"]),
        "production_cost": production_cost,
        "market_price": market_price,
        "total_revenue": total_revenue,
        "profit_margin": profit_margin,
        "sustainability_score": round(random.uniform(1, 10), 1),
        "timestamp": datetime.now().isoformat()
    }
    
    return data

@task
def produce_agri_data_to_kafka(**kwargs):
    """
    Task to produce synthetic agricultural data to Kafka.
    """
    # Get parameters from context
    num_records = kwargs.get('num_records', 100)
    topic = os.getenv('KAFKA_TOPIC', 'agri_data')
    
    # Create producer
    producer = get_kafka_producer()
    
    # Generate and send data
    for _ in range(num_records):
        data = generate_agri_data()
        producer.send(topic, value=data)
        time.sleep(0.1)  # Small delay to avoid overwhelming the system
    
    # Flush and close producer
    producer.flush()
    producer.close()
    
    return f"Produced {num_records} records to topic {topic}" 