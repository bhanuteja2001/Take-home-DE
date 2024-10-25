from confluent_kafka import Producer
import json
from datetime import datetime


def create_producer(config):
    """
    Creates a Kafka producer instance using the provided configuration.

    Args:
        config (dict): Configuration settings for the Kafka producer.

    Returns:
        Producer: A Kafka Producer instance.
    """
    return Producer(config)


def publish_message(producer, topic, message):
    """
    Publishes a message to a specified Kafka topic.

    Args:
        producer (Producer): The Kafka producer instance.
        topic (str): The topic to which the message will be published.
        message (str): The message content to be sent to Kafka.
    """
    producer.produce(topic, value=message)  # Send the message to the specified topic
    producer.flush()  # Ensure all messages are delivered before moving on


def log_error_to_kafka(producer, topic, user_id, error_type, msg_dict):
    """
    Logs an error message to a specified Kafka topic.

    Args:
        producer (Producer): The Kafka producer instance.
        topic (str): The topic where error logs will be sent.
        user_id (str): The ID of the user associated with the error.
        error_type (str): The type of error that occurred.
        msg_dict (dict): The original message content that caused the error.
    """
    # Create an error record with relevant information
    error_record = {
        "user_id": user_id,
        "error_type": error_type,
        "message_content": msg_dict,
        "timestamp": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),  # Capture the current timestamp
    }
    # Publish the error record to Kafka
    publish_message(producer, topic, json.dumps(error_record))


def log_cleaned_data(producer, topic, user_id, msg_dict, cleantype):
    """
    Logs cleaned data / the data that has been filtered out to a specified Kafka topic.

    Args:
        producer (Producer): The Kafka producer instance.
        topic (str): The topic where cleaned data will be sent.
        user_id (str): The ID of the user associated with the message.
        msg_dict (dict): The original message content that has been cleaned.
        cleantype (str): The type of cleaning that was performed on the data.
    """
    # Create a record of the cleaned data with relevant information
    record = {
        "user_id": user_id,
        "message_content": msg_dict,
        "cleantype": cleantype,
        "timestamp": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),  # Capture the current timestamp
    }
    # Publish the cleaned data record to Kafka
    publish_message(producer, topic, json.dumps(record))
