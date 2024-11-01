"""
Main module for the Kafka message processing pipeline.
"""

import json
import time
import uuid
from confluent_kafka import KafkaException, KafkaError
from consumer import create_consumer_with_retry, poll_message, close_consumer
from producer import create_producer, publish_message, flush_producer, log_error_to_kafka, log_cleaned_data
from summary_printer import SummaryPrinter
from transformer import transform_message
from metrics import MessageMetrics

def main():
    """Main function to run the Kafka message processing pipeline."""
    consumer_config = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': f'real-time-processor-group-{uuid.uuid4()}',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False
    }

    input_topic = 'user-login'

    main_producer_config = {
        'bootstrap.servers': 'localhost:29092',
        'acks': 'all',
        'compression.type': 'snappy',
        'retries': 3,
        'enable.idempotence': True
    }

    metrics_producer_config = {
        'bootstrap.servers': 'localhost:29092',
        'acks': 'all',
        'compression.type': 'snappy',
        'batch.size': 64768,
        'linger.ms': 80
    }

    summary_cleaned_producer_config = {
        'bootstrap.servers': 'localhost:29092',
        'acks': '1',
        'compression.type': 'snappy',
        'max.in.flight.requests.per.connection': 1
    }

    try:
        consumer = create_consumer_with_retry(consumer_config, input_topic)
    except KafkaException:
        print("Failed to create Kafka consumer after multiple attempts. Exiting.")
        return

    main_producer = create_producer(main_producer_config)
    metrics_producer = create_producer(metrics_producer_config)
    summary_cleaned_producer = create_producer(summary_cleaned_producer_config)

    summary_printer = SummaryPrinter()
    message_metrics = MessageMetrics()

    print("Kafka Consumer has started...")

    required_fields = ['user_id', 'ip', 'device_id', 'app_version', 'device_type', 'timestamp', 'locale']

    summary_counter = 0
    SUMMARY_PUBLISH_INTERVAL = 1000
    MAX_MESSAGE_AGE = 60

    try:
        while True:
            msg = poll_message(consumer)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            message_age = time.time() - msg.timestamp()[1] / 1000
            if message_age > MAX_MESSAGE_AGE:
                continue

            ## Increment with a counter : message received
            summary_printer.increment_received_count()


            ### Check for Parsing Error
            try:
                msg_dict = json.loads(msg.value().decode('utf-8'))
                original_timestamp = float(msg_dict['timestamp'])
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                log_error_to_kafka(main_producer, 'processed-errors', "Unknown", f"Message parsing error: {str(e)}", {})
                message_metrics.record_and_publish_metrics(metrics_producer, original_timestamp, 'processed-errors', "Unknown")
                continue

            ### Check if User_ID exists
            if 'user_id' not in msg_dict or not msg_dict['user_id']:
                log_error_to_kafka(main_producer, 'processed-errors', "Unknown", "Missing user_id", msg_dict)
                message_metrics.record_and_publish_metrics(metrics_producer, original_timestamp, 'processed-errors', "Unknown")
                continue

            ### Assigning user_id to message_id to avoid confusion
            message_id = msg_dict['user_id']

            ### Check for missing fields
            missing_fields = [field for field in required_fields if field not in msg_dict or msg_dict[field] is None]
            if missing_fields:
                log_error_to_kafka(main_producer, 'processed-errors', message_id, f"Missing fields: {', '.join(missing_fields)}", msg_dict)
                message_metrics.record_and_publish_metrics(metrics_producer, original_timestamp, 'processed-errors', message_id)
                continue

            ### Filter the message
            if msg_dict.get('app_version') != '2.3.0':
                summary_printer.increment_filtered_count()
                log_cleaned_data(summary_cleaned_producer, 'cleaned-data', message_id, msg_dict, 'filtered_app_version')
                message_metrics.record_and_publish_metrics(metrics_producer, original_timestamp, 'cleaned-data', message_id)
                continue

            ### Perform transformations
            transformed_msg, error = transform_message(msg_dict)
            if error:
                log_error_to_kafka(main_producer, 'processed-errors', message_id, f"Transformation error: {error}", msg_dict)
                message_metrics.record_and_publish_metrics(metrics_producer, original_timestamp, 'processed-errors', message_id)
                continue

            ## Increment : the received message is processed and update the counts
            summary_printer.increment_processed_count()
            summary_printer.update_counts(msg_dict['device_type'], msg_dict['locale'])

            ### PUBLISH the processed msg
            try:
                publish_message(main_producer, 'processed-output', transformed_msg)
                message_metrics.record_and_publish_metrics(metrics_producer, original_timestamp, 'processed-output', message_id)

                summary_counter += 1
                if summary_counter >= SUMMARY_PUBLISH_INTERVAL:
                    summary_stats = summary_printer.get_summary_statistics()
                    publish_message(summary_cleaned_producer, 'summary-output', summary_stats)
                    summary_counter = 0

                consumer.commit()
            except Exception as e:
                log_error_to_kafka(main_producer, 'processed-errors', message_id, f"Publishing error: {str(e)}", transformed_msg)
                message_metrics.record_and_publish_metrics(metrics_producer, original_timestamp, 'processed-errors', message_id)
                continue

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        close_consumer(consumer)
        flush_producer(main_producer)
        flush_producer(metrics_producer)
        flush_producer(summary_cleaned_producer)

if __name__ == "__main__":
    main()