from confluent_kafka import Consumer
import json

def main():
    # Configure the consumer
    consumer_config = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'metrics-reader-group',
        'auto.offset.reset': 'earliest'
    }

    # Create consumer instance
    consumer = Consumer(consumer_config)

    # Subscribe to the metrics-output topic
    consumer.subscribe(['metrics-output'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                # Decode and parse the JSON message
                metrics_data = json.loads(msg.value().decode('utf-8'))
                
                # Print the received metrics
                print("Received metrics:")
                for metric in metrics_data.get('message_details', []):
                    print(f"User ID: {metric['user_id']}")
                    print(f"Processing Time: {metric['processing_time']:.4f} seconds")
                    print(f"Topic: {metric['topic']}")
                    print(f"Timestamp: {metric['timestamp']}")
                    print("---")

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
            except KeyError as e:
                print(f"Missing expected key in metrics data: {e}")

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == "__main__":
    main()