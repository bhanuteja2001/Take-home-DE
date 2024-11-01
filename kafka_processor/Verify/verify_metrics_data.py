"""
Module for verifying Kafka metrics output.
"""

import json
import time
from typing import Dict, Any
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_END

def create_consumer_with_retry(config: Dict[str, Any], topic: str, max_retries: int = 5, retry_delay: int = 5) -> Consumer:
    """
    Create a Kafka consumer with retry mechanism.

    Args:
        config (Dict[str, Any]): Kafka consumer configuration.
        topic (str): Topic to subscribe to.
        max_retries (int): Maximum number of retry attempts.
        retry_delay (int): Delay between retries in seconds.

    Returns:
        Consumer: Configured Kafka consumer.

    Raises:
        Exception: If unable to create consumer after max retries.
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
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Unable to connect to Kafka broker.")
                raise

def main():
    """Main function to verify Kafka metrics."""
    consumer_config = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': f'metrics-verifier-{time.time()}',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False
    }

    topic = 'metrics-output'
    MAX_MESSAGE_AGE = 60

    try:
        consumer = create_consumer_with_retry(consumer_config, topic)

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                else:
                    print(f"Consumer error: {msg.error()}")
                continue

            message_age = time.time() - msg.timestamp()[1] / 1000
            if message_age > MAX_MESSAGE_AGE:
                print(f"Skipping old message. Age: {message_age:.2f} seconds")
                continue

            try:
                metrics_data = json.loads(msg.value().decode('utf-8'))
                print("Received metrics:")
                for metric in metrics_data.get('message_details', []):
                    print(f"Message ID: {metric.get('message_id', 'N/A')}")
                    print(f"Processing Time: {metric.get('processing_time', 'N/A'):.4f} seconds")
                    print(f"Topic: {metric.get('topic', 'N/A')}")
                    print(f"Timestamp: {metric.get('timestamp', 'N/A')}")
                    print("---")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
            except KeyError as e:
                print(f"Missing expected key in metrics data: {e}")

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()