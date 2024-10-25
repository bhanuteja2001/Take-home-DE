from confluent_kafka import Consumer
from error_producer import log_error_to_kafka

def create_consumer(config):
    try:
        consumer = Consumer(config)
        return consumer
    except Exception as e:
        log_error_to_kafka(None, 'error-log-topic', "Unknown", f'Consumer creation error: {str(e)}')
        raise

def subscribe(consumer, topic):
    consumer.subscribe([topic])

def poll_message(consumer):
    return consumer.poll(1.0)