from consumer import create_consumer, subscribe, poll_message
from producer import create_producer, produce_message
from transformer import transform_message
from logger import setup_logging, log_issue
from console_printer import ConsolePrinter
import json

def main():
    setup_logging()

    consumer_config = {
       'bootstrap.servers': 'localhost:29092',
       'group.id': 'my-consumer-group',
       'auto.offset.reset': 'earliest',
       'enable.auto.commit': False
    }

    producer_config = {
       'bootstrap.servers': 'localhost:29092'
    }

    consumer = create_consumer(consumer_config)
    producer = create_producer(producer_config)

    subscribe(consumer, 'user-login')

    printer = ConsolePrinter()

    try:
       while True:
           msg = poll_message(consumer)
           if msg is None or msg.error():
               continue

           msg_dict = json.loads(msg.value().decode('utf-8'))

           # Check for required fields
           required_fields = ['ip', 'device_id', 'app_version', 'device_type', 'timestamp', 'locale']
           all_fields_present = True
           for field in required_fields:
               if field not in msg_dict or msg_dict[field] is None:
                   user_id = msg_dict.get('user_id', 'Unknown')
                   log_issue(user_id, f"Missing {field} field")
                   printer.increment_missing_field_count(field)
                   all_fields_present = False

           if not all_fields_present:
               continue

           # Transform and filter the message
           transformed_msg = transform_message(msg.value().decode('utf-8'))
           if msg_dict.get('app_version') != '2.3.0':
               printer.increment_filtered_count()
               continue

           # Aggregate and produce the transformed message
           printer.update_counts(msg_dict['device_type'], msg_dict['locale'])
           produce_message(producer, 'processed-user-login', json.dumps(transformed_msg))

           # Increment processed record count
           printer.processed_count += 1

           # Print summary every 10000 processed messages without warnings
           if printer.processed_count % 10000 == 0:
               printer.print_summary()

    except KeyboardInterrupt:
       pass
    finally:
       consumer.close()

if __name__ == '__main__':
   main()