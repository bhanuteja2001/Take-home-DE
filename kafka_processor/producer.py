from confluent_kafka import Producer
import json
from datetime import datetime

def create_producer(config):
    return Producer(config)

def publish_message(producer, topic, message):
    producer.produce(topic, value=message)
    producer.flush()

def log_error_to_kafka(producer, topic, user_id, error_type, msg_dict):
    error_record = {
        'user_id': user_id,
        'error_type': error_type,
        'message_content': msg_dict,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    publish_message(producer, topic, json.dumps(error_record))

def log_cleaned_data(producer, topic, user_id, msg_dict, cleantype):
    record = {
        'user_id' : user_id,
        'message_content' : msg_dict,
        'cleantype' : cleantype,
        'timestamp' : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    publish_message(producer, topic, json.dumps(record))