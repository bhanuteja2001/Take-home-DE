from confluent_kafka import Producer

def create_producer(config):
    return Producer(config)

def produce_message(producer, topic, message):
    producer.produce(topic, value=message)
    producer.flush()