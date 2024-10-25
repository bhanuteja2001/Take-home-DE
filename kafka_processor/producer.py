from confluent_kafka import Producer
from error_producer import log_error_to_kafka

def create_producer(config):
    try:
        return Producer(config)
    except Exception as e:
        log_error_to_kafka(None, 'error-log-topic', "Unknown", f'Producer creation error: {str(e)}')
        raise

def produce_message(producer, topic, message):
    producer.produce(topic, value=message)
    producer.flush()