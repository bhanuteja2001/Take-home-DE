from confluent_kafka import Consumer, KafkaException

# Configuration for the Kafka Consumer
config = {
    'bootstrap.servers': 'localhost:29092',  # Use the external listener port
    'group.id': 'my-consumer-group',         # Unique group id for this consumer
    'auto.offset.reset': 'earliest',         # Start reading at the earliest message
    'enable.auto.commit': False              # Disable auto-commit of offsets
}

# Create a Consumer instance
consumer = Consumer(config)

# Subscribe to the topic
consumer.subscribe(['user-login'])

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for new messages with a timeout of 1 second
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            print(f'Received message: {msg.value().decode("utf-8")}')
except KeyboardInterrupt:
    pass
finally:
    # Close down consumer cleanly
    consumer.close()