from confluent_kafka import Consumer, KafkaError
import json
import os
import uuid
import datetime
import logging
import socket
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_temp_dir():
    """Set up temporary directory for storage."""
    import os
    local_temp_dir = "/tmp/agri_data"
    if not os.path.exists(local_temp_dir):
        os.makedirs(local_temp_dir)
    return local_temp_dir

def convert_json_to_parquet(messages, batch_id):
    """Convert a batch of JSON messages to Parquet format."""
    local_temp_dir = setup_temp_dir()
    
    # Check if we have any messages to process
    if not messages:
        logger.info("No messages to convert to Parquet.")
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
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    parquet_filename = f"{local_temp_dir}/agri_data_batch_{batch_id}_{timestamp}.parquet"
    
    # Write to Parquet file
    pq.write_table(table, parquet_filename)
    
    logger.info(f"Converted {len(messages)} messages to Parquet file: {parquet_filename}")
    
    return parquet_filename

def upload_to_gcs(parquet_file):
    """Upload Parquet file to Google Cloud Storage."""
    if parquet_file is None:
        logger.info("No Parquet file to upload.")
        return
    
    try:
        # Get GCS bucket and blob path from environment
        bucket_name = os.environ.get("GCS_BUCKET_NAME", "agri_data_bucket")
        blob_path = os.environ.get("GCS_RAW_DATA_PATH", "raw/agri_data/")
        
        # Create GCS client
        storage_client = storage.Client()
        
        # Get or create bucket
        try:
            bucket = storage_client.get_bucket(bucket_name)
        except Exception:
            bucket = storage_client.create_bucket(bucket_name)
            logger.info(f"Created new bucket: {bucket_name}")
        
        # Generate blob name from file name
        file_name = os.path.basename(parquet_file)
        blob_name = f"{blob_path}{file_name}"
        
        # Upload file to GCS
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(parquet_file)
        
        logger.info(f"File {file_name} uploaded to gs://{bucket_name}/{blob_name}")
        
        # Delete local file after upload
        os.remove(parquet_file)
        logger.info(f"Deleted local file {parquet_file}")
        
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")

def consume_messages(**kwargs):
    """Consume messages from Kafka and process them in batches."""
    local_temp_dir = setup_temp_dir()
    
    # Configuration
    kafka_broker = os.environ.get("KAFKA_BROKER", "broker:29092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "agri_data")
    consumer_group = os.environ.get("CONSUMER_GROUP", "agri_data_consumer")
    max_messages = int(os.environ.get("MAX_MESSAGES", 1000))
    batch_size = int(os.environ.get("BATCH_SIZE", 100))
    poll_timeout = float(os.environ.get("POLL_TIMEOUT", 1.0))
    
    # Configuration for the Kafka consumer
    consumer_config = {
        'bootstrap.servers': kafka_broker,
        'group.id': consumer_group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'debug': 'broker,topic,consumer'
    }
    
    logger.info(f"Consumer configuration: {consumer_config}")
    
    # Create Kafka consumer
    consumer = Consumer(consumer_config)
    consumer.subscribe([kafka_topic])
    
    logger.info(f"Starting to consume messages from topic {kafka_topic}")
    
    messages = []
    batch_counter = 0
    message_counter = 0
    batch_id = str(uuid.uuid4())[:8]
    
    try:
        while message_counter < max_messages:
            # Poll for messages
            msg = consumer.poll(poll_timeout)
            
            if msg is None:
                # No message received in this poll cycle
                if message_counter > 0 and message_counter % 10 == 0:
                    logger.info("No new messages, continuing to poll...")
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, not an error
                    logger.info(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Error consuming message: {msg.error()}")
                continue
            
            # Process message
            message_value = msg.value().decode('utf-8')
            messages.append(message_value)
            message_counter += 1
            
            logger.info(f"Consumed message {message_counter}/{max_messages} from partition {msg.partition()} offset {msg.offset()}")
            
            # Process a batch of messages when batch size is reached
            if len(messages) >= batch_size:
                logger.info(f"Processing batch {batch_counter + 1} with {len(messages)} messages")
                
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
            logger.info(f"Processing final batch with {len(messages)} messages")
            parquet_file = convert_json_to_parquet(messages, batch_id)
            upload_to_gcs(parquet_file)
            consumer.commit()
        
        logger.info(f"Finished consuming {message_counter} messages in {batch_counter + 1} batches")
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
    finally:
        # Clean up
        consumer.close()
        logger.info("Consumer closed")
    
    return {'status': 'success', 'message': f'Processed {message_counter} messages in {batch_counter + 1} batches'} 