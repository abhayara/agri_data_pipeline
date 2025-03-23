import json
import logging
from kafka import KafkaConsumer
from typing import Dict, List, Any
import time
from airflow.decorators import task
import os

logger = logging.getLogger(__name__)

def consume_from_kafka(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    auto_offset_reset: str = 'earliest',
    timeout_ms: int = 60000,
    max_records: int = 1000
) -> List[Dict[str, Any]]:
    """
    Consume messages from a Kafka topic and return as a list of dictionaries
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Topic to consume from
        group_id: Consumer group ID
        auto_offset_reset: Where to start consuming from
        timeout_ms: Consumer timeout in milliseconds
        max_records: Maximum number of records to consume
        
    Returns:
        List of dictionaries containing the consumed data
    """
    try:
        logger.info(f"Connecting to Kafka topic {topic} at {bootstrap_servers}")
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=timeout_ms
        )
        
        messages = []
        count = 0
        
        logger.info(f"Starting consumption from topic {topic}")
        for message in consumer:
            try:
                messages.append(message.value)
                count += 1
                
                if count >= max_records:
                    logger.info(f"Reached maximum record count: {max_records}")
                    break
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
                
        consumer.close()
        logger.info(f"Successfully consumed {len(messages)} messages from topic {topic}")
        
        return messages
        
    except Exception as e:
        logger.error(f"Error consuming from Kafka: {e}")
        raise

@task
def consume_from_kafka_task(topic, output_path, **kwargs):
    """
    Consume messages from Kafka and save to a local file
    
    Args:
        topic: Kafka topic to consume from
        output_path: Path to save the consumed messages
    
    Returns:
        Path to the saved file
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Consume messages
    messages = consume_from_kafka(
        bootstrap_servers='kafka:9092',
        topic=topic,
        group_id='my-group'
    )
    
    # Save messages to file
    with open(output_path, 'w') as f:
        json.dump(messages, f)
    
    return output_path
