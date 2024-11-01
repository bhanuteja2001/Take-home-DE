"""
This module provides functions to interact with Kafka producers.
"""

import json
import time
from confluent_kafka import Producer

def create_producer(config: dict) -> Producer:
    """
    Create a Kafka producer instance.

    Args:
        config (dict): Configuration dictionary for the producer.

    Returns:
        Producer: A Kafka Producer instance.
    """
    return Producer(config)

def publish_message(producer: Producer, topic: str, message: dict) -> None:
    """
    Publish a message to a Kafka topic.

    Args:
        producer (Producer): The Kafka producer instance.
        topic (str): The topic to publish to.
        message (dict): The message to publish.
    """
    producer.produce(topic, value=json.dumps(message))
    producer.poll(0)  # Trigger any callbacks

def flush_producer(producer: Producer) -> None:
    """
    Flush the producer to ensure all messages are sent.

    Args:
        producer (Producer): The Kafka producer instance to flush.
    """
    producer.flush()

def log_error_to_kafka(producer: Producer, topic: str, user_id: str, error_type: str, msg_dict: dict) -> None:
    """
    Log an error message to a Kafka topic.

    Args:
        producer (Producer): The Kafka producer instance.
        topic (str): The topic to publish the error to.
        user_id (str): The user ID associated with the error.
        error_type (str): The type of error that occurred.
        msg_dict (dict): The original message that caused the error.
    """
    error_record = {
        "user_id": user_id,
        "error_type": error_type,
        "message_content": msg_dict,
        "timestamp": int(time.time())
    }
    publish_message(producer, topic, error_record)

def log_cleaned_data(producer: Producer, topic: str, user_id: str, msg_dict: dict, clean_type: str) -> None:
    """
    Log cleaned data to a Kafka topic.

    Args:
        producer (Producer): The Kafka producer instance.
        topic (str): The topic to publish the cleaned data to.
        user_id (str): The user ID associated with the cleaned data.
        msg_dict (dict): The original message that was cleaned.
        clean_type (str): The type of cleaning that was performed.
    """
    cleaned_record = {
        "user_id": user_id,
        "message_content": msg_dict,
        "clean_type": clean_type,
        "timestamp": int(time.time())
    }
    publish_message(producer, topic, cleaned_record)