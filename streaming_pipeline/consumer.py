from confluent_kafka import Consumer, KafkaError
import json
from config import BOOTSTRAP_SERVERS, TOPIC

def consume_data(bootstrap_servers, topic):
    conf = {
        'bootstrap.servers': bootstrap_servers, 
        'group.id': 'agri-data-consumer', 
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    print(msg.error())
                    break
            else:
                # Process the agricultural data
                data = json.loads(msg.value().decode('utf-8'))
                print(f"Received data from farm: {data['farm_id']}, crop: {data['crop_type']}")
                print(f"Weather: {data['weather_temperature']}°C, {data['weather_rainfall']}mm rainfall")
                print(f"Expected yield: {data['expected_yield']} {data['yield_unit']}")
                print("---")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == '__main__':
    print(f"Starting consumer for topic {TOPIC} on {BOOTSTRAP_SERVERS}...")
    consume_data(BOOTSTRAP_SERVERS, TOPIC)
