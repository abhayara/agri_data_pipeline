from confluent_kafka import Consumer, KafkaError
import json
import os
import sys
import logging
import signal
import time
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv
from config import BOOTSTRAP_SERVERS, TOPIC, GROUP_ID, GCS_BUCKET, GCS_RAW_PATH

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('agri_data_consumer')

class GracefulShutdown:
    """Handle graceful shutdown on signals."""
    shutdown_requested = False
    
    def __init__(self):
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)
    
    def request_shutdown(self, *args):
        logger.info("Shutdown requested, completing current operations...")
        self.shutdown_requested = True

def save_to_gcs(data_batch, batch_id):
    """Save a batch of messages to Google Cloud Storage."""
    try:
        # Create GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        
        # Create a timestamp-based path
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        blob_name = f"{GCS_RAW_PATH}batch_{timestamp}_{batch_id}.json"
        blob = bucket.blob(blob_name)
        
        # Convert data to JSON and upload
        json_data = json.dumps(data_batch)
        blob.upload_from_string(json_data, content_type='application/json')
        
        logger.info(f"Saved batch {batch_id} with {len(data_batch)} messages to {blob_name}")
        return True
    except Exception as e:
        logger.error(f"Error saving to GCS: {e}")
        return False

def consume_data(max_messages=1000, batch_size=100, poll_timeout=1.0):
    """Consume data from Kafka and save to GCS in batches."""
    shutdown_handler = GracefulShutdown()
    
    try:
        # Consumer configuration
        consumer_conf = {
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'group.id': GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        
        # Create Consumer instance
        consumer = Consumer(consumer_conf)
        consumer.subscribe([TOPIC])
        logger.info(f"Connected to Kafka at {BOOTSTRAP_SERVERS}, subscribed to {TOPIC}")
        
        # Variables for batch processing
        messages_consumed = 0
        current_batch = []
        batch_count = 0
        
        # Main consumption loop
        while messages_consumed < max_messages and not shutdown_handler.shutdown_requested:
            # Poll for messages
            msg = consumer.poll(poll_timeout)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Process the message
            try:
                # Parse message value as JSON
                message_value = msg.value().decode('utf-8')
                message_data = json.loads(message_value)
                current_batch.append(message_data)
                messages_consumed += 1
                
                # If we've reached the batch size, save to GCS
                if len(current_batch) >= batch_size:
                    batch_count += 1
                    if save_to_gcs(current_batch, batch_count):
                        # Commit offsets only after successful save
                        consumer.commit()
                        logger.info(f"Committed offset after saving batch {batch_count}")
                    current_batch = []
                
                # Log progress periodically
                if messages_consumed % 100 == 0:
                    logger.info(f"Consumed {messages_consumed} messages so far")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
        
        # Save any remaining messages in the final batch
        if current_batch:
            batch_count += 1
            if save_to_gcs(current_batch, batch_count):
                consumer.commit()
        
        logger.info(f"Consumer finished. Consumed {messages_consumed} messages in {batch_count} batches")
        
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {e}")
    finally:
        try:
            # Properly close the consumer
            consumer.close()
            logger.info("Consumer closed")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")

if __name__ == "__main__":
    # Get parameters from environment
    max_messages = int(os.getenv('MAX_MESSAGES', 1000))
    batch_size = int(os.getenv('BATCH_SIZE', 100))
    poll_timeout = float(os.getenv('POLL_TIMEOUT', 1.0))
    
    logger.info(f"Starting agricultural data consumer: max={max_messages}, batch={batch_size}")
    consume_data(max_messages=max_messages, batch_size=batch_size, poll_timeout=poll_timeout)
