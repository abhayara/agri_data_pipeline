from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import logging
import sys
import os
import signal
import time
import traceback
from datetime import datetime
import pandas as pd
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('consumer.log')
    ]
)
logger = logging.getLogger('agri_data_consumer')

# Load configuration from environment variables
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'agri_data')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'agri_data_consumer')
AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'agri_data_bucket')
GCS_BLOB_PATH = os.getenv('GCS_TEMP_BLOB_PATH', 'raw/agri_data/')

class GracefulShutdown:
    """Handle graceful shutdown on signals."""
    shutdown_requested = False
    
    def __init__(self):
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)
    
    def request_shutdown(self, *args):
        logger.info("Shutdown requested, completing current operations...")
        self.shutdown_requested = True

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def connect_consumer(bootstrap_servers, topic, group_id, auto_offset_reset):
    """Connect to Kafka consumer with retry mechanism."""
    try:
        logger.info(f"Connecting to Kafka at {bootstrap_servers} for topic {topic}...")
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': False,  # Manual commit for better control
            'session.timeout.ms': 30000,  # 30 seconds
            'heartbeat.interval.ms': 10000,  # 10 seconds
            'max.poll.interval.ms': 300000,  # 5 minutes
            'fetch.max.bytes': 52428800,  # 50 MB
            'max.partition.fetch.bytes': 10485760  # 10 MB
        }
        consumer = Consumer(conf)
        consumer.subscribe([topic])
        logger.info(f"Successfully connected to Kafka and subscribed to topic {topic}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        logger.error(traceback.format_exc())
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def upload_to_gcs(df, bucket_name, blob_path):
    """Upload DataFrame to GCS with retry."""
    try:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        local_file = f"/tmp/agri_data_{timestamp}.csv"
        
        # Save DataFrame to local file
        df.to_csv(local_file, index=False)
        logger.info(f"Saved data to local file: {local_file}")
        
        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob_name = f"{blob_path}agri_data_{timestamp}.csv"
        blob = bucket.blob(blob_name)
        
        blob.upload_from_filename(local_file)
        logger.info(f"Uploaded data to GCS: gs://{bucket_name}/{blob_name}")
        
        # Clean up local file
        os.remove(local_file)
        logger.info(f"Removed local file: {local_file}")
        
        return f"gs://{bucket_name}/{blob_name}"
    except Exception as e:
        logger.error(f"Failed to upload data to GCS: {e}")
        logger.error(traceback.format_exc())
        raise

def consume_data(bootstrap_servers, topic, group_id=GROUP_ID, auto_offset_reset=AUTO_OFFSET_RESET, batch_size=100, flush_interval=60):
    """Consume agricultural data from Kafka topic with comprehensive error handling."""
    shutdown_handler = GracefulShutdown()
    consumer = None
    last_flush_time = time.time()
    messages = []
    processed_count = 0
    error_count = 0
    
    try:
        # Connect to Kafka with retry
        consumer = connect_consumer(bootstrap_servers, topic, group_id, auto_offset_reset)
        
        while not shutdown_handler.shutdown_requested:
            try:
                # Poll for messages with timeout
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No message received
                    current_time = time.time()
                    # Check if we need to flush based on time
                    if messages and (current_time - last_flush_time > flush_interval):
                        logger.info(f"Flush interval reached ({flush_interval}s), processing {len(messages)} messages")
                        process_messages_batch(messages, processed_count)
                        messages = []
                        last_flush_time = current_time
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        logger.debug(f"Reached end of partition {msg.partition()}")
                        continue
                    else:
                        # Real error
                        logger.error(f"Kafka error: {msg.error()}")
                        error_count += 1
                        if error_count > 10:
                            logger.error("Too many Kafka errors, stopping consumer")
                            break
                        continue
                
                # Reset error count on successful message
                error_count = 0
                
                # Process the message
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    messages.append(data)
                    processed_count += 1
                    
                    # Log progress periodically
                    if processed_count % 100 == 0:
                        logger.info(f"Processed {processed_count} messages so far")
                    
                    # If we've collected enough messages, process the batch
                    if len(messages) >= batch_size:
                        logger.info(f"Batch size reached ({batch_size}), processing messages")
                        process_messages_batch(messages, processed_count)
                        messages = []
                        last_flush_time = time.time()
                    
                    # Commit offset after processing
                    consumer.commit(msg)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message as JSON: {e}")
                    logger.error(f"Message value: {msg.value()}")
                    consumer.commit(msg)  # Commit anyway to move past bad message
                
            except KafkaException as e:
                logger.error(f"Kafka exception: {e}")
                logger.error(traceback.format_exc())
                # Retry connecting if possible
                if "Connection refused" in str(e) or "Broker transport failure" in str(e):
                    logger.info("Attempting to reconnect to Kafka...")
                    time.sleep(5)
                    consumer = connect_consumer(bootstrap_servers, topic, group_id, auto_offset_reset)
                
        # Process any remaining messages before shutdown
        if messages:
            logger.info(f"Processing {len(messages)} remaining messages before shutdown")
            process_messages_batch(messages, processed_count)
        
        logger.info(f"Consumer shutting down, processed {processed_count} messages total")
        return processed_count
        
    except RetryError:
        logger.error("Failed to connect to Kafka after multiple retries")
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {e}")
        logger.error(traceback.format_exc())
    finally:
        if consumer:
            try:
                consumer.close()
                logger.info("Consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
        
        return processed_count

def process_messages_batch(messages, total_processed):
    """Process a batch of messages and upload to GCS."""
    if not messages:
        logger.warning("No messages to process")
        return
    
    try:
        logger.info(f"Processing batch of {len(messages)} messages")
        
        # Convert to DataFrame
        df = pd.DataFrame(messages)
        
        # Upload to GCS if enabled
        if GCS_BUCKET_NAME and GCS_BUCKET_NAME != 'None':
            upload_to_gcs(df, GCS_BUCKET_NAME, GCS_BLOB_PATH)
        else:
            logger.info("GCS upload disabled or not configured")
            logger.info(f"Data sample (5 rows):\n{df.head().to_string()}")
        
        logger.info(f"Successfully processed batch of {len(messages)} messages (total: {total_processed})")
    except Exception as e:
        logger.error(f"Error processing message batch: {e}")
        logger.error(traceback.format_exc())

if __name__ == '__main__':
    try:
        logger.info(f"Starting consumer for topic {TOPIC} on {BOOTSTRAP_SERVERS}")
        logger.info(f"Using group ID: {GROUP_ID}, offset reset: {AUTO_OFFSET_RESET}")
        
        # Get batch size and flush interval from environment or use defaults
        batch_size = int(os.getenv('BATCH_SIZE', '100'))
        flush_interval = int(os.getenv('FLUSH_INTERVAL_SECONDS', '60'))
        
        logger.info(f"Batch size: {batch_size}, flush interval: {flush_interval} seconds")
        
        if GCS_BUCKET_NAME and GCS_BUCKET_NAME != 'None':
            logger.info(f"Will upload data to GCS bucket: {GCS_BUCKET_NAME}, path: {GCS_BLOB_PATH}")
        else:
            logger.info("GCS upload is disabled")
        
        processed_count = consume_data(
            BOOTSTRAP_SERVERS, 
            TOPIC, 
            GROUP_ID, 
            AUTO_OFFSET_RESET,
            batch_size,
            flush_interval
        )
        
        logger.info(f"Consumer finished, processed {processed_count} messages")
        sys.exit(0)
        
    except Exception as e:
        logger.critical(f"Fatal error in consumer: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
