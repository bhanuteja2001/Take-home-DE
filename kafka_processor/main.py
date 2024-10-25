from consumer import create_consumer, subscribe, poll_message
from producer import create_producer, publish_message, log_error_to_kafka, log_cleaned_data
from transformer import transform_message
from summary_printer import SummaryPrinter
import json

def main():
    # Configuration settings for the Kafka consumer
    consumer_config = {
        'bootstrap.servers': 'localhost:29092',  # Kafka broker address.
        'group.id': 'real-time-processor-group',  # Unique identifier for the consumer group.
        'auto.offset.reset': 'earliest',  # Start consuming from the earliest message if no offset is found.
        'enable.auto.commit': False,  # Disable automatic offset commits for manual control after processing. If True, then can lead to potential message loss if your application crashes before processing all messages since the offset is committed automatically
    }

    # Configuration settings for the Kafka producer
    producer_config = {
        'bootstrap.servers': 'localhost:29092',  # Kafka broker address.
        'acks': 'all',  # It ensures that all in-sync replicas acknowledge the message before considering it successfully sent. This minimizes the risk of data loss but may increase latency slightly due to waiting for all replicas.
        'retries': 5,  # Retry sending messages up to 5 times on transient errors to improve reliability.
        'linger.ms': 5,  # Introduce a small delay to allow for batching, improving throughput.
        'batch.size': 16384,  # Set a reasonable batch size (16 KB) to optimize throughput without excessive memory use.
        'compression.type': 'snappy'  # Use Snappy compression for fast processing and reduced message size.
    }

    # Create consumer and producer instances using the above configurations
    consumer = create_consumer(consumer_config)
    producer = create_producer(producer_config)


    # Subscribe the consumer to the 'user-login' topic
    subscribe(consumer, 'user-login')

    # Initialize a summary printer to keep track of processing statistics
    printer = SummaryPrinter()


    print("Kafka Consumer has started...")

    try:
        # Start an infinite loop to continuously poll for new messages
        while True:
            # Poll for a new message with a timeout
            msg = poll_message(consumer)
            if msg is None or msg.error(): # Check for message availability and errors
                continue

            try:
                # Decode and load the message value as a JSON dictionary
                msg_dict = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError as e:
                # Log JSON decoding errors to Kafka
                log_error_to_kafka(producer, 'processed-errors', "Unknown", f"JSON decoding error: {str(e)}", {})
                continue

            # Check for required fields, specifically 'user_id'
            user_id = msg_dict.get('user_id')
            if user_id is None:
                log_error_to_kafka(producer, 'processed-errors', "Unknown", "Missing user_id", msg_dict)
                continue

            # List of required fields to check for completeness
            required_fields = ['ip', 'device_id', 'app_version', 'device_type', 'timestamp', 'locale']
            all_fields_present = True
            missing_fields = []
            for field in required_fields:
                if field not in msg_dict or msg_dict[field] is None:
                    missing_fields.append(field)
                    all_fields_present = False

            # Log an error if any required fields are missing
            if not all_fields_present:
                log_error_to_kafka(producer, 'processed-errors', user_id, f"Missing fields: {missing_fields}", msg_dict)
                continue

            # Filter out messages where 'app_version' is not '2.3.0'
            if msg_dict.get('app_version') != '2.3.0':
                printer.increment_filtered_count()
                #log_cleaned_data(producer, 'cleaned-data', user_id, msg_dict, 'filterd messages app_version != 2.3.0')
                continue
            
            # Transform the message (e.g., encode PII)
            transformed_msg = transform_message(msg_dict, producer, 'processed-errors')
            if transformed_msg is None:
                continue
            
            # Update the summary statistics
            printer.update_counts(msg_dict['device_type'], msg_dict['locale'])
            
            try:
                # Publish the transformed message to the 'processed-output' topic
                publish_message(producer, 'processed-output', json.dumps(transformed_msg))
                consumer.commit(asynchronous=False)  # Commit after successful publishing
            except Exception as e:
                # Log any publishing errors to Kafka
                log_error_to_kafka(producer, 'processed-errors', user_id, f"Publishing error: {str(e)}", transformed_msg)
                continue  # Skip to the next message in case of failure

            # Increment the processed message count
            printer.processed_count += 1
            #print(printer.processed_count)

            # Publish summary statistics every 1000 processed messages
            if printer.processed_count % 1000 == 0:
                statistics = printer.get_summary_statistics()
                publish_message(producer, 'summary-output', json.dumps(statistics))


    except KeyboardInterrupt:
        print("Shutting down consumer...") # Graceful shutdown message
    
    finally:
        consumer.close()  # Ensure the consumer is closed
        producer.flush()  # No close method; just flush to ensure delivery


if __name__ == '__main__':
    main()