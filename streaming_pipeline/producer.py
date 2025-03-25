from confluent_kafka import Producer
from config import BOOTSTRAP_SERVERS, TOPIC
import json
import random
from datetime import datetime, timedelta
import time
from faker import Faker

fake = Faker()

# Function to generate synthetic agricultural data
def generate_agri_data():
    """Generate a random agricultural data point."""
    farm_types = ['Organic', 'Conventional', 'Hydroponic', 'Vertical', 'Mixed']
    crop_types = ['Corn', 'Wheat', 'Soybeans', 'Rice', 'Barley', 'Cotton', 'Potatoes', 'Tomatoes']
    crop_varieties = ['Hybrid', 'GMO', 'Heirloom', 'Early', 'Late', 'Drought-resistant']
    soil_types = ['Clay', 'Sandy', 'Loamy', 'Silty', 'Peaty', 'Chalky', 'Volcanic']
    yield_units = ['tons/hectare', 'kg/acre', 'bushels/acre', 'pounds/sq-ft']
    
    farm_id = f"FARM-{random.randint(1000, 9999)}"
    farm_type = random.choice(farm_types)
    farm_size = random.randint(10, 5000)
    
    crop_id = f"CROP-{random.randint(100, 999)}"
    crop_type = random.choice(crop_types)
    crop_variety = random.choice(crop_varieties)
    
    # Realistic date ranges
    current_year = datetime.now().year
    planting_date = fake.date_between(start_date=f'{current_year}-03-01', end_date=f'{current_year}-05-30')
    harvest_date = fake.date_between(start_date=f'{current_year}-08-01', end_date=f'{current_year}-11-30')
    
    # Weather metrics with some correlation to yield
    weather_temp = round(random.uniform(5, 40), 1)  # Celsius
    weather_humidity = round(random.uniform(30, 95), 1)  # Percentage
    weather_rainfall = round(random.uniform(0, 300), 1)  # mm
    weather_sunlight = round(random.uniform(3, 14), 1)  # hours
    
    soil_type = random.choice(soil_types)
    soil_ph = round(random.uniform(5.0, 8.5), 1)
    soil_moisture = round(random.uniform(10, 90), 1)  # Percentage
    
    # Create realistic correlation between conditions and yields
    base_yield = random.uniform(1, 10)
    # Better conditions = higher yields
    weather_factor = (1 + (weather_temp / 100) - (abs(weather_temp - 25) / 50))
    moisture_factor = (1 + (soil_moisture / 200))
    expected_yield = round(base_yield * weather_factor * moisture_factor, 2)
    
    # Actual yield sometimes differs from expected
    variation = random.uniform(0.8, 1.2)
    actual_yield = round(expected_yield * variation, 2)
    yield_unit = random.choice(yield_units)
    
    # Financial metrics
    production_cost = round(random.uniform(100, 5000) * (farm_size / 100), 2)
    market_price = round(random.uniform(50, 500), 2)
    total_revenue = round(actual_yield * market_price, 2)
    profit_margin = round((total_revenue - production_cost) / total_revenue * 100, 2)
    
    # Sustainability score (higher is better)
    sustainability_score = round(random.uniform(1, 10), 1)
    
    # Current timestamp
    timestamp = datetime.now().isoformat()
    
    return {
        "farm_id": farm_id,
        "farm_type": farm_type,
        "farm_size_acres": farm_size,
        "crop_id": crop_id,
        "crop_type": crop_type,
        "crop_variety": crop_variety,
        "planting_date": planting_date.strftime('%Y-%m-%d'),
        "harvest_date": harvest_date.strftime('%Y-%m-%d'),
        "weather_temperature": weather_temp,
        "weather_humidity": weather_humidity,
        "weather_rainfall": weather_rainfall,
        "weather_sunlight": weather_sunlight,
        "soil_type": soil_type,
        "soil_ph": soil_ph,
        "soil_moisture": soil_moisture,
        "expected_yield": expected_yield,
        "actual_yield": actual_yield,
        "yield_unit": yield_unit,
        "production_cost": production_cost,
        "market_price": market_price,
        "total_revenue": total_revenue,
        "profit_margin": profit_margin,
        "sustainability_score": sustainability_score,
        "timestamp": timestamp
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()}')


def produce_data(bootstrap_servers, topic, num_messages=100, delay=0.5):
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    try:
        for _ in range(num_messages):
            # Generate agricultural data
            data = generate_agri_data()
            
            # Convert to JSON string
            json_data = json.dumps(data)

            # Produce the message to the specified Kafka topic
            producer.produce(topic, value=json_data, callback=delivery_report)
            
            # Flush the producer to ensure the message is sent
            producer.poll(0)
            
            # Add delay between messages
            time.sleep(delay)
            
        # Final flush to ensure all messages are delivered
        producer.flush()
        print(f"Successfully produced {num_messages} agricultural data records to topic {topic}")

    except KeyboardInterrupt:
        print("Producer interrupted by user")
    except Exception as e:
        print(f"Error producing data: {e}")
    finally:
        producer.flush()


if __name__ == '__main__':
    num_messages = 1000  # Reduced from 5000 to 1000 messages
    delay = 0.05  # Reduced from 0.5 seconds to 0.05 seconds for faster streaming
    produce_data(BOOTSTRAP_SERVERS, TOPIC, num_messages, delay)
