from confluent_kafka import Consumer


def create_consumer(config):
    """
    Creates a Kafka consumer instance using the provided configuration.

    Args:
        config (dict): Configuration settings for the Kafka consumer.

    Returns:
        Consumer: A Kafka Consumer instance.
    """
    return Consumer(config)  # Return a new instance of the Consumer


def subscribe(consumer, topic):
    """
    Subscribes the consumer to a specified Kafka topic.

    Args:
        consumer (Consumer): The Kafka consumer instance.
        topic (str): The topic to subscribe to.
    """
    consumer.subscribe([topic])  # Subscribe to the given topic


def poll_message(consumer):
    """
    Polls for a new message from the Kafka topic.

    Args:
        consumer (Consumer): The Kafka consumer instance.

    Returns:
        Message or None: The polled message if available, otherwise None.
    """
    return consumer.poll(1.0)  # Poll for messages with a timeout of 1 second
