import hashlib
from producer import log_error_to_kafka

def transform_message(msg_dict, producer, error_topic):
    try:
        msg_dict['ip'] = hash_value(msg_dict['ip'])
        msg_dict['device_id'] = hash_value(msg_dict['device_id'])
        msg_dict['timestamp'] = format_timestamp(msg_dict['timestamp'])
    except Exception as e:
        user_id = msg_dict.get('user_id', 'Unknown')
        log_error_to_kafka(producer, error_topic, user_id, f"Transformation error: {str(e)}", msg_dict)
        return None
    
    return msg_dict

def hash_value(value):
    return hashlib.sha256(value.encode()).hexdigest()

def format_timestamp(unix_timestamp):
    from datetime import datetime
    return datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')