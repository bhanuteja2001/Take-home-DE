### Read Topic : processed-errors

from confluent_kafka import Consumer

# Configure the consumer
consumer_config = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['processed-errors'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()