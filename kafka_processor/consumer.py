from confluent_kafka import Consumer

def create_consumer(config):
    consumer = Consumer(config)
    return consumer

def subscribe(consumer, topic):
    consumer.subscribe([topic])

def poll_message(consumer):
    return consumer.poll(1.0)