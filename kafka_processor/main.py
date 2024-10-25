from consumer import create_consumer, subscribe, poll_message
from producer import create_producer, produce_message
from transformer import transform_message
from error_producer import create_producer as create_error_producer, log_error_to_kafka
from console_printer import ConsolePrinter
import json

def main():
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    producer_config = {
        'bootstrap.servers': 'localhost:9092'
    }

    consumer = create_consumer(consumer_config)
    producer = create_producer(producer_config)
    error_producer = create_error_producer(producer_config)

    subscribe(consumer, 'user-login')

    printer = ConsolePrinter()

    print("Kafka Consumer has started...")

    try:
        while True:
            msg = poll_message(consumer)
            if msg is None or msg.error():
                continue

            msg_dict = json.loads(msg.value().decode('utf-8'))

            # Check for required fields
            required_fields = ['ip', 'device_id', 'app_version', 'device_type', 'timestamp', 'locale']
            all_fields_present = True
            missing_fields = []
            for field in required_fields:
                if field not in msg_dict or msg_dict[field] is None:
                    missing_fields.append(field)
                    all_fields_present = False

            if not all_fields_present:
                user_id = msg_dict.get('user_id', 'Unknown')
                log_error_to_kafka(error_producer, 'error-log-topic', user_id, f"Missing fields: {missing_fields}")
                continue

            # Transform and filter the message
            transformed_msg = transform_message(msg.value().decode('utf-8'), error_producer, 'error-log-topic')
            if transformed_msg is None:
                continue

            # Aggregate and produce the transformed message
            printer.update_counts(msg_dict['device_type'], msg_dict['locale'])
            produce_message(producer, 'processed-user-login', json.dumps(transformed_msg))

            # Increment processed record count
            printer.processed_count += 1

            # Print summary every 1000 processed messages without warnings
            if printer.processed_count % 1000 == 0:
                printer.print_summary()

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    
    finally:
        consumer.close()

if __name__ == '__main__':
    main()