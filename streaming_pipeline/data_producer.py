#!/usr/bin/env python3
import json
import time
import random
import datetime
from confluent_kafka import Producer
import os
import socket
import uuid
import csv
import sys

# Configuration
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "broker:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "agri_data")
NUM_MESSAGES = int(os.environ.get("NUM_MESSAGES", 100))
MESSAGE_DELAY = float(os.environ.get("MESSAGE_DELAY", 0.5))

# Configuration for the Kafka producer
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': socket.gethostname(),
    'message.timeout.ms': 10000,  # 10 seconds timeout
    'request.timeout.ms': 5000,   # 5 seconds timeout
    'retry.backoff.ms': 500,      # Retry every 500ms
    'socket.keepalive.enable': True,
    'debug': 'broker' # Reduce verbosity
}

print(f"Producer configuration: {producer_config}")
print(f"Producer will send {NUM_MESSAGES} messages to topic {KAFKA_TOPIC} with delay {MESSAGE_DELAY}s")
print(f"Broker address: {KAFKA_BROKER}")

# Try to resolve the broker hostname
try:
    print(f"Attempting to resolve broker hostname...")
    broker_host = KAFKA_BROKER.split(':')[0]
    broker_ip = socket.gethostbyname(broker_host)
    print(f"✅ Resolved {broker_host} to {broker_ip}")
except Exception as e:
    print(f"❌ Failed to resolve broker hostname: {e}")

# Kafka delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# Function to load the agricultural data schema
def load_schema():
    schema = {}
    
    try:
        with open('/home/streaming_pipeline/data/DescriptionDataCoAgri.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if len(row) >= 2:
                    field = row[0].strip()
                    description = row[1].strip()
                    schema[field] = description
    except Exception as e:
        print(f"Error loading schema: {e}")
        # If file not found, use a default schema
        schema = {
            "Farm_ID": "Unique identifier for the farm",
            "Farm_Type": "Type of farm: Crop, Livestock, Mixed, Horticulture",
            "Crop_Type": "Type of crop: Grain, Vegetable, Fruit, Fiber, Oilseed",
            "Soil_Type": "Type of soil: Clay, Sandy, Loam, Silt, Peaty",
            # Add more schema elements as needed
        }
    
    return schema

# Function to generate random farm data
def generate_farm_data():
    farm_id = f"FARM_{random.randint(1000, 9999)}"
    farm_types = ["Crop", "Livestock", "Mixed", "Horticulture"]
    farm_size = random.uniform(10.0, 1000.0)
    lat = random.uniform(20.0, 50.0)
    lon = random.uniform(-130.0, -70.0)
    location = f"{lat:.6f},{lon:.6f}"
    
    return {
        "Farm_ID": farm_id,
        "Farm_Type": random.choice(farm_types),
        "Farm_Size_Acres": round(farm_size, 2),
        "Farm_Location": location,
        "Farmer_ID": f"FARMER_{random.randint(1000, 9999)}",
        "Farmer_Name": f"Farmer {random.choice(['John', 'Jane', 'Bob', 'Alice', 'Carlos', 'Maria'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller'])}",
        "Farmer_Contact": f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
    }

# Function to generate random crop data
def generate_crop_data():
    crop_types = {
        "Grain": ["Wheat", "Corn", "Rice", "Barley", "Oats"],
        "Vegetable": ["Tomato", "Potato", "Carrot", "Onion", "Lettuce", "Broccoli", "Cabbage", "Spinach"],
        "Fruit": ["Apple", "Orange", "Banana", "Grape", "Strawberry", "Mango", "Watermelon", "Peach"],
        "Fiber": ["Cotton", "Hemp", "Flax", "Jute", "Sisal"],
        "Oilseed": ["Soybean", "Canola", "Sunflower", "Peanut", "Sesame"]
    }
    
    crop_type = random.choice(list(crop_types.keys()))
    crop_variety = random.choice(crop_types[crop_type])
    
    # Generate random dates for planting and harvesting
    today = datetime.date.today()
    
    # More realistic planting seasons based on crop type
    if crop_type == "Grain":
        # Spring planting for most grains
        planting_month = random.randint(3, 5)  # March to May
    elif crop_type == "Vegetable":
        # Spring or early summer planting for vegetables
        planting_month = random.randint(4, 6)  # April to June
    elif crop_type == "Fruit":
        # Trees planted in fall or spring
        planting_month = random.choice([3, 4, 9, 10])  # March, April, September, October
    elif crop_type == "Fiber":
        # Spring planting for fiber crops
        planting_month = random.randint(3, 5)  # March to May
    else:  # Oilseed
        # Spring planting for oilseeds
        planting_month = random.randint(3, 5)  # March to May
    
    planting_day = random.randint(1, 28)
    planting_date = datetime.date(today.year, planting_month, planting_day)
    
    # Calculate harvest date based on crop type (realistic growing periods)
    if crop_type == "Grain":
        # 3-4 month growing period
        growing_days = random.randint(90, 120)
    elif crop_type == "Vegetable":
        # 1-3 month growing period
        growing_days = random.randint(30, 90)
    elif crop_type == "Fruit":
        # 4-6 month growing period for annual fruits
        growing_days = random.randint(120, 180)
    elif crop_type == "Fiber":
        # 3-5 month growing period
        growing_days = random.randint(90, 150)
    else:  # Oilseed
        # 3-4 month growing period
        growing_days = random.randint(90, 120)
    
    harvest_date = (planting_date + datetime.timedelta(days=growing_days)).strftime("%Y-%m-%d")
    planting_date = planting_date.strftime("%Y-%m-%d")
    
    # Generate realistic yields based on crop type
    if crop_type == "Grain":
        expected_yield = random.uniform(2.5, 8.0)  # tons per hectare
        yield_unit = "ton/ha"
    elif crop_type == "Vegetable":
        expected_yield = random.uniform(15.0, 50.0)  # tons per hectare
        yield_unit = "ton/ha"
    elif crop_type == "Fruit":
        expected_yield = random.uniform(10.0, 40.0)  # tons per hectare
        yield_unit = "ton/ha"
    elif crop_type == "Fiber":
        expected_yield = random.uniform(1.0, 3.0)  # tons per hectare
        yield_unit = "ton/ha"
    else:  # Oilseed
        expected_yield = random.uniform(1.0, 4.0)  # tons per hectare
        yield_unit = "ton/ha"
    
    # Actual yield varies from expected but with realistic variation
    yield_variation = random.uniform(0.8, 1.1)  # Between 80% and 110% of expected
    actual_yield = expected_yield * yield_variation
    
    return {
        "Crop_ID": f"CROP_{random.randint(1000, 9999)}",
        "Crop_Type": crop_type,
        "Crop_Variety": crop_variety,
        "Planting_Date": planting_date,
        "Harvest_Date": harvest_date,
        "Growing_Period_Days": growing_days,
        "Expected_Yield": round(expected_yield, 2),
        "Actual_Yield": round(actual_yield, 2),
        "Yield_Unit": yield_unit
    }

# Function to generate random soil data
def generate_soil_data():
    soil_types = ["Clay", "Sandy", "Loam", "Silt", "Peaty", "Chalky", "Silty Clay", "Sandy Loam"]
    soil_textures = ["Fine", "Medium", "Coarse"]
    drainage_ratings = ["Poor", "Moderate", "Good", "Excellent"]
    
    # Select a soil type
    soil_type = random.choice(soil_types)
    
    # Generate pH appropriate for the soil type
    if soil_type == "Clay":
        pH = round(random.uniform(5.0, 7.5), 1)
    elif soil_type == "Sandy":
        pH = round(random.uniform(5.5, 7.0), 1)
    elif soil_type == "Loam":
        pH = round(random.uniform(6.0, 7.0), 1)
    elif soil_type == "Silt":
        pH = round(random.uniform(6.0, 7.0), 1)
    elif soil_type == "Peaty":
        pH = round(random.uniform(4.0, 6.0), 1)
    elif soil_type == "Chalky":
        pH = round(random.uniform(7.0, 8.5), 1)
    else:
        pH = round(random.uniform(5.5, 7.5), 1)
    
    # Generate realistic NPK levels
    nitrogen = round(random.uniform(10.0, 100.0), 1)  # ppm
    phosphorus = round(random.uniform(5.0, 50.0), 1)  # ppm
    potassium = round(random.uniform(20.0, 200.0), 1)  # ppm
    
    # Generate realistic organic matter content
    organic_matter = round(random.uniform(1.0, 8.0), 1)  # percentage
    
    return {
        "Soil_Type": soil_type,
        "Soil_Texture": random.choice(soil_textures),
        "Soil_pH": pH,
        "Soil_Nitrogen": nitrogen,
        "Soil_Phosphorus": phosphorus,
        "Soil_Potassium": potassium,
        "Soil_Organic_Matter": organic_matter,
        "Soil_Drainage": random.choice(drainage_ratings),
        "Soil_Moisture": round(random.uniform(10.0, 50.0), 1),
        "Soil_Temperature": round(random.uniform(15.0, 30.0), 1),
        "Soil_Fertility_Rating": random.randint(1, 10)
    }

# Function to generate random weather data
def generate_weather_data():
    # Define seasons
    current_month = datetime.date.today().month
    if current_month in [12, 1, 2]:
        season = "Winter"
    elif current_month in [3, 4, 5]:
        season = "Spring"
    elif current_month in [6, 7, 8]:
        season = "Summer"
    else:
        season = "Fall"
    
    # Generate temperature based on season
    if season == "Winter":
        temperature = round(random.uniform(-5.0, 15.0), 1)
    elif season == "Spring":
        temperature = round(random.uniform(5.0, 25.0), 1)
    elif season == "Summer":
        temperature = round(random.uniform(15.0, 35.0), 1)
    else:  # Fall
        temperature = round(random.uniform(0.0, 20.0), 1)
    
    # Generate rainfall based on season
    if season == "Winter":
        rainfall = round(random.uniform(20.0, 100.0), 1)
    elif season == "Spring":
        rainfall = round(random.uniform(30.0, 150.0), 1)
    elif season == "Summer":
        rainfall = round(random.uniform(10.0, 80.0), 1)
    else:  # Fall
        rainfall = round(random.uniform(40.0, 200.0), 1)
    
    # Generate wind speed
    wind_speed = round(random.uniform(0.0, 30.0), 1)
    
    return {
        "Weather_Season": season,
        "Weather_Temperature": temperature,
        "Weather_Humidity": round(random.uniform(30.0, 90.0), 1),
        "Weather_Rainfall": rainfall,
        "Weather_Sunlight_Hours": round(random.uniform(2.0, 12.0), 1),
        "Weather_Wind_Speed": wind_speed,
        "Weather_Extreme_Events": random.choice(["None", "None", "None", "None", "Frost", "Flood", "Drought", "Hail", "Strong Wind"])
    }

# Function to generate random irrigation data
def generate_irrigation_data():
    irrigation_methods = ["Drip", "Sprinkler", "Flood", "Center Pivot", "Sub-surface", "None"]
    method = random.choice(irrigation_methods)
    
    water_sources = ["Well", "River", "Reservoir", "Municipal", "Rainwater Collection"]
    efficiency_ratings = ["Low", "Medium", "High"]
    
    if method == "None":
        amount = 0
        frequency = "None"
        water_source = "None"
        efficiency = "None"
    else:
        # Different methods have different typical amounts
        if method == "Drip":
            amount = round(random.uniform(2.0, 8.0), 2)  # mm/day
            efficiency = "High"
        elif method == "Sprinkler":
            amount = round(random.uniform(5.0, 15.0), 2)  # mm/day
            efficiency = random.choice(["Medium", "High"])
        elif method == "Flood":
            amount = round(random.uniform(20.0, 50.0), 2)  # mm/day
            efficiency = "Low"
        elif method == "Center Pivot":
            amount = round(random.uniform(10.0, 30.0), 2)  # mm/day
            efficiency = "Medium"
        else:  # Sub-surface
            amount = round(random.uniform(3.0, 10.0), 2)  # mm/day
            efficiency = "High"
        
        frequencies = ["Daily", "Every 2-3 days", "Weekly", "Bi-weekly"]
        frequency = random.choice(frequencies)
        water_source = random.choice(water_sources)
    
    return {
        "Irrigation_Method": method,
        "Irrigation_Amount": amount,
        "Irrigation_Unit": "mm/day" if amount > 0 else "None",
        "Irrigation_Frequency": frequency,
        "Irrigation_Water_Source": water_source,
        "Irrigation_Efficiency": efficiency,
        "Irrigation_Automation": random.choice(["Manual", "Semi-automated", "Fully Automated"]) if method != "None" else "None"
    }

# Function to generate random fertilizer and pesticide data
def generate_fertilizer_pesticide_data():
    fertilizer_types = ["Organic", "Chemical", "Mixed", "None"]
    pesticide_types = ["Herbicide", "Insecticide", "Fungicide", "None"]
    
    fertilizer_type = random.choice(fertilizer_types)
    pesticide_type = random.choice(pesticide_types)
    
    fertilizer_amount = 0 if fertilizer_type == "None" else round(random.uniform(50.0, 500.0), 2)
    pesticide_amount = 0 if pesticide_type == "None" else round(random.uniform(1.0, 20.0), 2)
    
    return {
        "Fertilizer_Type": fertilizer_type,
        "Fertilizer_Amount": fertilizer_amount,
        "Pesticide_Type": pesticide_type,
        "Pesticide_Amount": pesticide_amount
    }

# Function to generate random equipment and labor data
def generate_equipment_labor_data():
    equipments = ["Tractor", "Harvester", "Drone", "Manual"]
    
    return {
        "Equipment_Used": random.choice(equipments),
        "Labor_Hours": round(random.uniform(10.0, 100.0), 1)
    }

# Function to generate random financial data
def generate_financial_data():
    production_cost = round(random.uniform(500.0, 5000.0), 2)
    market_price = round(random.uniform(10.0, 100.0), 2)
    revenue = round(random.uniform(1000.0, 10000.0), 2)
    profit_margin = round((revenue - production_cost) / revenue * 100, 2)
    
    return {
        "Production_Cost": production_cost,
        "Market_Price": market_price,
        "Total_Revenue": revenue,
        "Profit_Margin": profit_margin
    }

# Function to generate random storage and transportation data
def generate_storage_transportation_data():
    storage_methods = ["Silo", "Warehouse", "Cold Storage", "None"]
    transportation_methods = ["Truck", "Train", "Ship", "Air"]
    
    return {
        "Storage_Method": random.choice(storage_methods),
        "Storage_Duration": random.randint(0, 365),
        "Transportation_Method": random.choice(transportation_methods),
        "Transportation_Distance": random.randint(10, 5000)
    }

# Function to generate random quality and certification data
def generate_quality_certification_data():
    quality_grades = ["Premium", "Standard", "Basic"]
    certifications = ["Organic", "Fair Trade", "GAP", "None"]
    
    return {
        "Quality_Grade": random.choice(quality_grades),
        "Certification": random.choice(certifications)
    }

# Function to generate random sustainability data
def generate_sustainability_data():
    return {
        "Carbon_Footprint": round(random.uniform(100.0, 1000.0), 2),
        "Water_Footprint": round(random.uniform(500.0, 5000.0), 2),
        "Sustainability_Score": random.randint(1, 10)
    }

# Function to generate random market and customer data
def generate_market_customer_data():
    markets = ["Local", "Regional", "National", "International"]
    customers = ["Retailer", "Processor", "Wholesaler", "Direct Consumer"]
    
    return {
        "Market_Destination": random.choice(markets),
        "Customer_Segment": random.choice(customers)
    }

# Function to generate random order data
def generate_order_data():
    today = datetime.date.today()
    days_ago = random.randint(1, 60)
    order_date = (today - datetime.timedelta(days=days_ago)).strftime("%Y-%m-%d")
    delivery_date = (today - datetime.timedelta(days=random.randint(0, days_ago-1))).strftime("%Y-%m-%d")
    
    statuses = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]
    payment_methods = ["Cash", "Credit", "Bank Transfer", "Digital Wallet"]
    payment_statuses = ["Pending", "Completed", "Failed", "Refunded"]
    
    return {
        "Order_ID": f"ORDER_{random.randint(10000, 99999)}",
        "Order_Date": order_date,
        "Delivery_Date": delivery_date,
        "Order_Status": random.choice(statuses),
        "Payment_Method": random.choice(payment_methods),
        "Payment_Status": random.choice(payment_statuses)
    }

# Function to generate a complete agricultural record
def generate_record():
    record = {}
    record.update(generate_farm_data())
    record.update(generate_crop_data())
    record.update(generate_soil_data())
    record.update(generate_weather_data())
    record.update(generate_irrigation_data())
    record.update(generate_fertilizer_pesticide_data())
    record.update(generate_equipment_labor_data())
    record.update(generate_financial_data())
    record.update(generate_storage_transportation_data())
    record.update(generate_quality_certification_data())
    record.update(generate_sustainability_data())
    record.update(generate_market_customer_data())
    record.update(generate_order_data())
    
    # Add a unique message ID and timestamp
    record["message_id"] = str(uuid.uuid4())
    record["timestamp"] = datetime.datetime.now().isoformat()
    
    return record

def main():
    # Create Kafka producer
    producer = None
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"Attempt {retry_count + 1} to create Kafka producer...")
            producer = Producer(producer_config)
            # Test the connection
            producer.list_topics(timeout=5.0)
            print("✅ Successfully connected to Kafka")
            break
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            retry_count += 1
            if retry_count >= max_retries:
                print("Failed to connect to Kafka after maximum retries")
                sys.exit(1)
            print(f"Retrying in {retry_count} seconds...")
            time.sleep(retry_count)
    
    print(f"Starting to produce {NUM_MESSAGES} messages to topic {KAFKA_TOPIC}")
    
    # Generate and produce messages
    for i in range(NUM_MESSAGES):
        try:
            # Generate a record
            record = generate_record()
            
            # Serialize the record to JSON
            message = json.dumps(record).encode('utf-8')
            
            # Print a sample of the messages (first 2 and every 10th)
            if i < 2 or i % 10 == 0:
                print(f"Producing message {i+1}/{NUM_MESSAGES}: {record['Farm_ID']} - {record['Crop_Type']} - {record['Crop_Variety']}")
            
            # Produce the message to Kafka with the callback
            producer.produce(KAFKA_TOPIC, key=str(i), value=message, callback=delivery_report)
            
            # Flush to make sure the message is sent
            if i % 10 == 0:
                producer.flush(timeout=5.0)
            
            # Wait a bit between messages to simulate real-time data
            time.sleep(MESSAGE_DELAY)
            
        except Exception as e:
            print(f"Error producing message {i}: {e}")
            time.sleep(1)
    
    # Flush any remaining messages
    producer.flush(timeout=10.0)
    print("All messages produced successfully!")

if __name__ == "__main__":
    main() 