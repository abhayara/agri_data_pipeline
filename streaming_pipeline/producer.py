from confluent_kafka import Producer
import json
import random
from datetime import datetime, timedelta
import time
import logging
import sys
import os
import signal
import traceback
from faker import Faker
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('producer.log')
    ]
)
logger = logging.getLogger('agri_data_producer')

# Load configuration from environment variables
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'agri_data')

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
    except Exception as e:
        logger.error(f"Error generating agricultural data: {e}")
        logger.error(traceback.format_exc())
        # Return minimal valid data in case of error
        return {
            "farm_id": f"FARM-ERROR-{random.randint(1000, 9999)}",
            "crop_type": "Unknown",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

def delivery_report(err, msg):
    """Callback function for Kafka producer to report on message delivery."""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()}')

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
def connect_producer(bootstrap_servers):
    """Connect to Kafka with retry mechanism."""
    try:
        logger.info(f"Connecting to Kafka at {bootstrap_servers}...")
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'message.timeout.ms': 10000,  # 10 seconds
            'request.timeout.ms': 30000,  # 30 seconds
            'retry.backoff.ms': 500,      # 0.5 seconds between retries
            'message.send.max.retries': 5 # Retry up to 5 times
        }
        producer = Producer(producer_config)
        # Test connection by getting metadata
        producer.list_topics(timeout=10)
        logger.info("Successfully connected to Kafka")
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        logger.error(traceback.format_exc())
        raise

def produce_data(bootstrap_servers, topic, num_messages=100, delay=0.5):
    """Produce agricultural data to Kafka topic with comprehensive error handling."""
    shutdown_handler = GracefulShutdown()
    producer = None
    messages_produced = 0
    messages_failed = 0
    
    try:
        # Connect to Kafka with retry
        producer = connect_producer(bootstrap_servers)
        
        # Main production loop
        for i in range(num_messages):
            if shutdown_handler.shutdown_requested:
                logger.info("Shutdown requested, stopping message production")
                break
                
            try:
                # Generate agricultural data
                data = generate_agri_data()
                
                # Convert to JSON string
                json_data = json.dumps(data)

            # Produce the message to the specified Kafka topic
                producer.produce(topic, value=json_data, callback=delivery_report)
                
                # Periodically flush to ensure messages are sent
                if i % 10 == 0:
                    producer.poll(0)
                    
                messages_produced += 1
                
                # Log progress periodically
                if messages_produced % 100 == 0:
                    logger.info(f"Progress: {messages_produced}/{num_messages} messages produced")
                
                # Add delay between messages
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error producing message #{i}: {e}")
                logger.error(traceback.format_exc())
                messages_failed += 1
                # If too many consecutive failures, break
                if messages_failed > 10:
                    logger.error("Too many consecutive failures, stopping production")
                    break
        
        # Final flush to ensure all messages are delivered
        producer.flush(timeout=30)
        success_rate = (messages_produced - messages_failed) / max(messages_produced, 1) * 100
        logger.info(f"Completed: {messages_produced} messages produced, {messages_failed} failed, {success_rate:.2f}% success rate")

    except RetryError:
        logger.error("Failed to connect to Kafka after multiple retries")
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error producing data: {e}")
        logger.error(traceback.format_exc())
    finally:
        if producer:
            try:
                # Ensure any remaining messages are sent
                producer.flush(timeout=30)
                logger.info("Final flush completed")
            except Exception as e:
                logger.error(f"Error during final flush: {e}")
                
        logger.info(f"Producer finished. Messages: {messages_produced} produced, {messages_failed} failed")
        return messages_produced, messages_failed

if __name__ == '__main__':
    try:
        # Ensure these can be overridden with environment variables
        num_messages = int(os.getenv('NUM_MESSAGES', '1000'))
        delay = float(os.getenv('MESSAGE_DELAY', '0.05'))
        
        logger.info(f"Starting producer for topic {TOPIC} on {BOOTSTRAP_SERVERS}")
        logger.info(f"Will produce {num_messages} messages with {delay}s delay between messages")
        
        messages_produced, messages_failed = produce_data(BOOTSTRAP_SERVERS, TOPIC, num_messages, delay)
        
        if messages_failed > 0:
            exit_code = 1
            logger.warning(f"Completed with {messages_failed} failed messages")
        else:
            exit_code = 0
            logger.info("Completed successfully")
            
        sys.exit(exit_code)
        
    except Exception as e:
        logger.critical(f"Fatal error in producer: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
