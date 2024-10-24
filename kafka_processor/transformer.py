import json
import hashlib
from datetime import datetime

def transform_message(message):
    msg_dict = json.loads(message)
    msg_dict['ip'] = hash_value(msg_dict['ip'])
    msg_dict['device_id'] = hash_value(msg_dict['device_id'])
    msg_dict['timestamp'] = format_timestamp(msg_dict['timestamp'])
    return msg_dict

def hash_value(value):
    return hashlib.sha256(value.encode()).hexdigest()

def format_timestamp(unix_timestamp):
    return datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')