from confluent_kafka import Producer
import json
from datetime import datetime

def create_producer(config):
    return Producer(config)

def log_error_to_kafka(producer, topic, user_id, error_type):
    if producer is None:
        print(f"Error logging failed: {error_type}")
        return
    
    error_record = {
        'user_id': user_id,
        'error_type': error_type,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    producer.produce(topic, key=user_id, value=json.dumps(error_record))
    producer.flush()