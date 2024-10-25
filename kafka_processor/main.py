from consumer import create_consumer, subscribe, poll_message
from producer import create_producer, publish_message, log_error_to_kafka
from transformer import transform_message
from summary_printer import SummaryPrinter
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

    subscribe(consumer, 'user-login')

    printer = SummaryPrinter()

    print("Kafka Consumer has started...")

    try:
        while True:
            msg = poll_message(consumer)
            if msg is None or msg.error():
                continue

            try:
                msg_dict = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError as e:
                log_error_to_kafka(producer, 'processed-errors', "Unknown", f"JSON decoding error: {str(e)}", {})
                continue

            # Check for user_id specifically
            user_id = msg_dict.get('user_id')
            if user_id is None:
                log_error_to_kafka(producer, 'processed-errors', "Unknown", "Missing user_id", msg_dict)
                continue

            # Check for other required fields
            required_fields = ['ip', 'device_id', 'app_version', 'device_type', 'timestamp', 'locale']
            all_fields_present = True
            missing_fields = []
            for field in required_fields:
                if field not in msg_dict or msg_dict[field] is None:
                    missing_fields.append(field)
                    all_fields_present = False

            if not all_fields_present:
                log_error_to_kafka(producer, 'processed-errors', user_id, f"Missing fields: {missing_fields}", msg_dict)
                continue

            transformed_msg = transform_message(msg_dict, producer, 'processed-errors')
            if transformed_msg is None:
                continue

            printer.update_counts(msg_dict['device_type'], msg_dict['locale'])
            publish_message(producer, 'processed-output', json.dumps(transformed_msg))

            printer.processed_count += 1

            if printer.processed_count % 1000 == 0:
                statistics = printer.get_summary_statistics()
                publish_message(producer, 'summary-output', json.dumps(statistics))

                errors = printer.get_summary_errors()
                publish_message(producer, 'summary-errors', json.dumps(errors))

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    
    finally:
        consumer.close()

if __name__ == '__main__':
    main()