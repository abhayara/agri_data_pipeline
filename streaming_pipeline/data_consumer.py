#!/usr/bin/env python3
import json
import time
import os
import sys
import uuid
import socket
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import logging
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to resolve broker host to IP
def resolve_broker_ip(broker_host):
    """Attempt to resolve broker hostname to IP address with retry logic."""
    max_retries = 5
    retry_count = 0
    backoff_time = 1
    
    while retry_count < max_retries:
        try:
            logger.info(f"Attempting to resolve broker hostname: {broker_host}")
            hostname = broker_host.split(':')[0]
            broker_ip = socket.gethostbyname(hostname)
            logger.info(f"✅ Successfully resolved {hostname} to {broker_ip}")
            
            # Update hosts file as a fallback
            try:
                with open('/etc/hosts', 'r') as f:
                    hosts_content = f.read()
                
                if hostname not in hosts_content:
                    logger.info(f"Adding {hostname} to /etc/hosts")
                    with open('/etc/hosts', 'a') as f:
                        f.write(f"{broker_ip} {hostname}\n")
            except Exception as e:
                logger.warning(f"Could not update /etc/hosts: {e}")
                
            return broker_ip
        except socket.gaierror as e:
            logger.warning(f"Failed to resolve broker hostname (attempt {retry_count+1}/{max_retries}): {e}")
            retry_count += 1
            
            if retry_count >= max_retries:
                logger.error(f"Failed to resolve broker hostname after {max_retries} attempts")
                return None
                
            # Exponential backoff with jitter
            sleep_time = backoff_time + random.uniform(0, 1)
            logger.info(f"Retrying in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            backoff_time *= 2
    
    return None

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BROKER", "broker:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "agri_data")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "agri_data_consumer_new")
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", 1000))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 50))
POLL_TIMEOUT = float(os.environ.get("POLL_TIMEOUT", 5.0))
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "agri_data_tf_bucket")
GCS_RAW_DATA_PATH = os.environ.get("GCS_RAW_DATA_PATH", "raw/agri_data/")
LOCAL_TEMP_DIR = "/tmp/agri_data"
GCP_CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/home/streaming_pipeline/gcp-creds.json")

# Try to resolve broker hostname
broker_ip = resolve_broker_ip(KAFKA_BOOTSTRAP_SERVERS)

# Configuration for the Kafka consumer with better timeout settings
consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': KAFKA_AUTO_OFFSET_RESET,
    'enable.auto.commit': False,
    'message.timeout.ms': 30000,  # 30 seconds timeout
    'session.timeout.ms': 60000,  # 60 seconds session timeout
    'request.timeout.ms': 30000,  # 30 seconds timeout
    'socket.keepalive.enable': True,
    'socket.timeout.ms': 30000,  # 30 seconds socket timeout
    'retry.backoff.ms': 1000,    # Backoff time between retries
    'reconnect.backoff.ms': 1000, # Initial backoff time for reconnection
    'reconnect.backoff.max.ms': 10000, # Maximum backoff time for reconnection
    'debug': 'consumer,broker'
}

logger.info(f"Consumer configuration: {consumer_config}")
logger.info(f"Consumer will read from topic {KAFKA_TOPIC}")
logger.info(f"Broker address: {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"GCS Settings - Bucket: {GCS_BUCKET_NAME}, Path: {GCS_RAW_DATA_PATH}")

def setup_temp_dir():
    """Create a temporary directory for storing data before uploading to GCS."""
    global LOCAL_TEMP_DIR
    if not os.path.exists(LOCAL_TEMP_DIR):
        try:
            os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
            print(f"Created temporary directory: {LOCAL_TEMP_DIR}")
            # Ensure directory is accessible
            os.chmod(LOCAL_TEMP_DIR, 0o777)
            print(f"Set permissions on temporary directory: {LOCAL_TEMP_DIR}")
        except Exception as e:
            print(f"Error creating temporary directory: {e}")
            # Try an alternative location if /tmp is not accessible
            alt_dir = os.path.join(os.getcwd(), "data_output")
            if not os.path.exists(alt_dir):
                os.makedirs(alt_dir, exist_ok=True)
                os.chmod(alt_dir, 0o777)
            LOCAL_TEMP_DIR = alt_dir
            print(f"Using alternative temporary directory: {LOCAL_TEMP_DIR}")
    return LOCAL_TEMP_DIR

def convert_json_to_parquet(messages, batch_id):
    """Convert a batch of JSON messages to Parquet format."""
    # Check if we have any messages to process
    if not messages:
        print("No messages to convert to Parquet.")
        return None
    
    # Parse the first message to get the schema
    first_message = json.loads(messages[0])
    schema_fields = []
    
    # Create PyArrow schema from the first message
    for key, value in first_message.items():
        if isinstance(value, int):
            field_type = pa.int64()
        elif isinstance(value, float):
            field_type = pa.float64()
        elif isinstance(value, bool):
            field_type = pa.bool_()
        else:
            field_type = pa.string()
        
        schema_fields.append(pa.field(key, field_type))
    
    schema = pa.schema(schema_fields)
    
    # Parse all messages and create arrays
    parsed_messages = [json.loads(msg) for msg in messages]
    arrays = []
    
    # Create arrays for each field
    for field in schema_fields:
        field_name = field.name
        field_values = [msg.get(field_name, None) for msg in parsed_messages]
        arrays.append(pa.array(field_values, field.type))
    
    # Create table and write to Parquet
    table = pa.Table.from_arrays(arrays, schema=schema)
    
    # Generate a timestamp for the filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    parquet_filename = f"{LOCAL_TEMP_DIR}/agri_data_batch_{batch_id}_{timestamp}.parquet"
    
    # Write to Parquet file
    pq.write_table(table, parquet_filename)
    
    print(f"Converted {len(messages)} messages to Parquet file: {parquet_filename}")
    
    # Sample data display
    print("\nSAMPLE DATA (First 3 records):")
    for i, msg in enumerate(parsed_messages[:3]):
        print(f"Record {i+1}: {json.dumps(msg, indent=2)[:150]}...")
    
    return parquet_filename

def upload_to_gcs(parquet_file):
    """Upload a Parquet file to Google Cloud Storage."""
    if not parquet_file or not os.path.exists(parquet_file):
        print("No valid Parquet file to upload.")
        return
    
    try:
        print(f"Using GCS credentials from {GCP_CREDENTIALS_PATH}")
        client = storage.Client.from_service_account_json(GCP_CREDENTIALS_PATH)
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        print(f"Uploading {parquet_file} to gs://{GCS_BUCKET_NAME}/{GCS_RAW_DATA_PATH}")
        
        # Generate blob name from file name
        file_name = os.path.basename(parquet_file)
        blob_name = f"{GCS_RAW_DATA_PATH}{file_name}"
        
        # Upload file to GCS
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(parquet_file)
        
        print(f"✅ File {file_name} uploaded to gs://{GCS_BUCKET_NAME}/{blob_name}")
        
        # Delete local file after upload
        os.remove(parquet_file)
        print(f"✅ Deleted local file {parquet_file}")
        
        return f"gs://{GCS_BUCKET_NAME}/{blob_name}"
    except Exception as e:
        print(f"❌ Error uploading to GCS: {e}")
        return None

def consume_messages():
    """Consume messages from Kafka and process them in batches."""
    setup_temp_dir()
    
    # Create Kafka consumer with improved retry logic
    consumer = None
    max_retries = 10
    retry_count = 0
    backoff_time = 1
    
    while retry_count < max_retries:
        try:
            logger.info(f"Attempt {retry_count + 1} to create Kafka consumer...")
            consumer = Consumer(consumer_config)
            consumer.subscribe([KAFKA_TOPIC])
            
            # Test connectivity with graceful timeout handling
            msg = consumer.poll(10.0)
            if msg is None:
                logger.info("No messages available during connectivity test, but connection succeeded")
            elif msg.error():
                logger.warning(f"Connection test returned error: {msg.error()}")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Reached end of partition - this is not an error")
                else:
                    raise Exception(f"Kafka error: {msg.error()}")
            else:
                logger.info(f"Successfully received a message from topic {KAFKA_TOPIC}")
                
            logger.info("✅ Successfully connected to Kafka and subscribed to topic")
            break
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            if consumer:
                consumer.close()
                consumer = None
                
            retry_count += 1
            if retry_count >= max_retries:
                logger.error("Failed to connect to Kafka after maximum retries")
                sys.exit(1)
                
            # Exponential backoff with jitter
            sleep_time = backoff_time + random.uniform(0, 1)
            logger.info(f"Retrying in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            backoff_time = min(backoff_time * 2, 60)  # Cap at 60 seconds
            
            # Resolve broker IP again on failure
            broker_ip = resolve_broker_ip(KAFKA_BOOTSTRAP_SERVERS)
            if broker_ip:
                # Update consumer config with resolved IP
                host, port = KAFKA_BOOTSTRAP_SERVERS.split(':')
                consumer_config['bootstrap.servers'] = f"{host}:{port}"
                logger.info(f"Updated bootstrap.servers to {consumer_config['bootstrap.servers']}")
    
    logger.info(f"Starting to consume messages from topic {KAFKA_TOPIC}")
    
    messages = []
    batch_counter = 0
    message_counter = 0
    batch_id = str(uuid.uuid4())[:8]
    last_message_time = time.time()
    
    try:
        while message_counter < MAX_MESSAGES:
            # Poll for messages
            msg = consumer.poll(POLL_TIMEOUT)
            
            # Process timeouts - if we have messages but no new ones for a while,
            # process the batch we have
            if msg is None:
                if messages and (time.time() - last_message_time > 10):  # 10 seconds with no new messages
                    print(f"No new messages for 10 seconds, processing current batch of {len(messages)} messages")
                    parquet_file = convert_json_to_parquet(messages, batch_id)
                    upload_to_gcs(parquet_file)
                    messages = []
                    batch_counter += 1
                    batch_id = str(uuid.uuid4())[:8]
                    last_message_time = time.time()
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, not an error
                    print(f"Reached end of partition {msg.partition()}")
                else:
                    print(f"❌ Error consuming message: {msg.error()}")
                continue
            
            # Process message
            message_value = msg.value().decode('utf-8')
            messages.append(message_value)
            message_counter += 1
            last_message_time = time.time()
            
            if message_counter % 10 == 0:
                print(f"✅ Consumed message {message_counter}/{MAX_MESSAGES} from partition {msg.partition()} offset {msg.offset()}")
            
            # Process a batch of messages when batch size is reached
            if len(messages) >= BATCH_SIZE:
                print(f"Processing batch {batch_counter + 1} with {len(messages)} messages")
                
                # Convert to Parquet and upload to GCS
                parquet_file = convert_json_to_parquet(messages, batch_id)
                upload_to_gcs(parquet_file)
                
                # Reset messages and increment batch counter
                messages = []
                batch_counter += 1
                batch_id = str(uuid.uuid4())[:8]
                
                # Commit offsets
                consumer.commit()
        
        # Process any remaining messages
        if messages:
            print(f"Processing final batch with {len(messages)} messages")
            parquet_file = convert_json_to_parquet(messages, batch_id)
            upload_to_gcs(parquet_file)
            consumer.commit()
        
        print(f"Finished consuming {message_counter} messages in {batch_counter + 1} batches")
    
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        # Clean up
        if consumer:
            consumer.close()
            print("Consumer closed")

def main():
    """Main function to run the consumer."""
    print("Starting Kafka to GCS consumer")
    
    # Set up the GCS credentials
    if not os.path.exists(GCP_CREDENTIALS_PATH):
        print(f"Warning: GOOGLE_APPLICATION_CREDENTIALS file does not exist at {GCP_CREDENTIALS_PATH}")

    # Start consuming messages
    consume_messages()

if __name__ == "__main__":
    main() 