import json
import hashlib
from datetime import datetime
from error_producer import log_error_to_kafka

def transform_message(message, producer, error_topic):
    try:
        msg_dict = json.loads(message)
    except json.JSONDecodeError as e:
        user_id = "Unknown"
        log_error_to_kafka(producer, error_topic, user_id, f'JSON decoding error: {str(e)}')
        return None

    try:
        msg_dict['ip'] = hash_value(msg_dict['ip'])
        msg_dict['device_id'] = hash_value(msg_dict['device_id'])
        msg_dict['timestamp'] = format_timestamp(msg_dict['timestamp'])
    except Exception as e:
        user_id = msg_dict.get('user_id', 'Unknown')
        log_error_to_kafka(producer, error_topic, user_id, f'Transformation error: {str(e)}')
        return None
    
    return msg_dict

def hash_value(value):
    return hashlib.sha256(value.encode()).hexdigest()

def format_timestamp(unix_timestamp):
    return datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')