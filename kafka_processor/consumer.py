"""
This module provides functions to interact with Kafka consumer.
"""

from typing import Dict, Any
from confluent_kafka import Consumer, KafkaException, TopicPartition, OFFSET_END
import time

def create_consumer_with_retry(config: Dict[str, Any], topic: str, max_retries: int = 5, retry_delay: int = 5) -> Consumer:
    """
    Create a Kafka consumer with retry mechanism and seek to end.

    Args:
        config (Dict[str, Any]): Kafka consumer configuration.
        topic (str): Topic to subscribe to.
        max_retries (int): Maximum number of retry attempts.
        retry_delay (int): Delay between retries in seconds.

    Returns:
        Consumer: Configured Kafka consumer.

    Raises:
        KafkaException: If unable to create consumer after max retries.
    """
    for attempt in range(max_retries):
        try:
            consumer = Consumer(config)
            consumer.subscribe([topic])
            
            while not consumer.assignment():
                consumer.poll(0.1)
            
            for partition in consumer.assignment():
                consumer.seek(TopicPartition(topic, partition.partition, OFFSET_END))
            
            print(f"Successfully connected to Kafka broker on attempt {attempt + 1}")
            return consumer
        except KafkaException as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Unable to connect to Kafka broker.")
                raise

def poll_message(consumer: Consumer, timeout: float = 1.0):
    """
    Poll for a new message from the subscribed topics.

    Args:
        consumer (Consumer): The Kafka consumer instance.
        timeout (float): The maximum time to block waiting for a message.

    Returns:
        Message: A message object, or None on timeout.
    """
    return consumer.poll(timeout)

def close_consumer(consumer: Consumer) -> None:
    """
    Close the consumer connection.

    Args:
        consumer (Consumer): The Kafka consumer instance to close.
    """
    consumer.close()