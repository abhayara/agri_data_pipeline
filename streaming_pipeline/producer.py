from confluent_kafka import Producer
import json
import random
from datetime import datetime, timedelta
import time
import logging
import sys
import os
import signal
from faker import Faker
from dotenv import load_dotenv
from config import BOOTSTRAP_SERVERS, TOPIC, AGRI_SCHEMA

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('agri_data_producer')

fake = Faker()

class GracefulShutdown:
    """Handle graceful shutdown on signals."""
    shutdown_requested = False
    
    def __init__(self):
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)
    
    def request_shutdown(self, *args):
        logger.info("Shutdown requested, completing current operations...")
        self.shutdown_requested = True

# Function to generate synthetic agricultural data
def generate_agri_data():
    """Generate a random agricultural data point."""
    try:
        farm_id = f"FARM-{random.randint(1000, 9999)}"
        farm_name = fake.company() + " Farms"
        farm_location = fake.city()
        farm_size = round(random.uniform(10, 5000), 1)
        
        crop_id = f"CROP-{random.randint(100, 999)}"
        crop_types = ['Corn', 'Wheat', 'Soybeans', 'Rice', 'Barley', 'Cotton']
        crop_type = random.choice(crop_types)
        
        # Date ranges
        current_year = datetime.now().year
        planting_date = fake.date_between(
            start_date=datetime(current_year, 3, 1),
            end_date=datetime(current_year, 5, 30)
        ).strftime('%Y-%m-%d')
        
        harvest_date = fake.date_between(
            start_date=datetime(current_year, 8, 1),
            end_date=datetime(current_year, 11, 30)
        ).strftime('%Y-%m-%d')
        
        # Weather metrics
        weather_temp = round(random.uniform(10, 35), 1)  # Celsius
        weather_humidity = round(random.uniform(30, 90), 1)  # Percentage
        weather_rainfall = round(random.uniform(0, 250), 1)  # mm
        
        soil_types = ['Clay', 'Sandy', 'Loamy', 'Silty', 'Peaty']
        soil_type = random.choice(soil_types)
        soil_ph = round(random.uniform(5.5, 7.5), 1)
        
        # Yield data
        expected_yield = round(random.uniform(2, 8), 2)
        actual_yield = round(expected_yield * random.uniform(0.8, 1.2), 2)
        yield_units = ['tons/hectare', 'kg/acre', 'bushels/acre']
        yield_unit = random.choice(yield_units)
        
        # Sustainability score (higher is better)
        sustainability_score = round(random.uniform(1, 10), 1)
        
        # Current timestamp
        timestamp = datetime.now().isoformat()
        
        return {
            "farm_id": farm_id,
            "farm_name": farm_name,
            "farm_location": farm_location,
            "farm_size_acres": farm_size,
            "crop_id": crop_id,
            "crop_type": crop_type,
            "planting_date": planting_date,
            "harvest_date": harvest_date,
            "weather_temperature": weather_temp,
            "weather_humidity": weather_humidity,
            "weather_rainfall": weather_rainfall,
            "soil_type": soil_type,
            "soil_ph": soil_ph,
            "expected_yield": expected_yield,
            "actual_yield": actual_yield,
            "yield_unit": yield_unit,
            "sustainability_score": sustainability_score,
            "timestamp": timestamp
        }
    except Exception as e:
        logger.error(f"Error generating agricultural data: {e}")
        return None

def delivery_report(err, msg):
    """Callback for Kafka producer to report on message delivery."""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()}')

def produce_data(num_messages=100, delay=1.0):
    """Produce agricultural data to Kafka topic."""
    shutdown_handler = GracefulShutdown()
    
    try:
        # Producer configuration
        producer_config = {
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'client.id': 'agri-data-producer'
        }
        
        # Create Producer instance
        producer = Producer(producer_config)
        logger.info(f"Connected to Kafka at {BOOTSTRAP_SERVERS}")
        
        # Main production loop
        messages_produced = 0
        while messages_produced < num_messages and not shutdown_handler.shutdown_requested:
            # Generate data
            data = generate_agri_data()
            if data:
                # Convert to JSON
                message = json.dumps(data)
                
                # Produce message
                producer.produce(
                    TOPIC,
                    key=data['farm_id'],
                    value=message,
                    callback=delivery_report
                )
                
                # Poll to handle callbacks
                producer.poll(0)
                messages_produced += 1
                
                if messages_produced % 10 == 0:
                    logger.info(f"Produced {messages_produced} messages")
                
                # Add delay between messages
                time.sleep(delay)
        
        # Flush producer
        logger.info("Flushing producer...")
        producer.flush()
        logger.info(f"Successfully produced {messages_produced} messages to {TOPIC}")
        
    except Exception as e:
        logger.error(f"Error in producer: {e}")
    finally:
        logger.info("Producer finished")

if __name__ == "__main__":
    # Get number of messages and delay from command args
    num_messages = int(os.getenv('NUM_MESSAGES', 100))
    delay = float(os.getenv('MESSAGE_DELAY', 1.0))
    
    logger.info(f"Starting agricultural data producer: {num_messages} messages with {delay}s delay")
    produce_data(num_messages=num_messages, delay=delay)
