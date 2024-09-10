from kafka import KafkaConsumer
import json
from kafka.errors import KafkaError

from Data_Ingestion_Service import kafkaProduer

consumer = KafkaConsumer(
    'api_query',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='foo_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=True,
    consumer_timeout_ms=1000  # One second timeout.
)

def get_next_message():
    try:
        print("Consuming messages from broker")
        message = next(consumer, None)
        if message:
            print(f"Message from partition {message.partition}, {message.offset}")
            print(f"Message value:{message.value}")
            return message.value
        else:
            print("No message received within timeout period")
            return None
    except KafkaError as e:
        print(f"Caught a KafkaException:{e}")
    except StopIteration:
        print("No more messages to consume")
        return None

def consume_and_trigger():
    """Consumes messages from Kafka and triggers send_task to fill the data (This is an easy way to update data too)
    deprecated went a different route instead of integrating this with the api call itself, I went to integrate it with the deco """
    try:
        no_message_count = 0  # counter to check,
        print("Consuming messages from broker...")

        for message in consumer:
            print(f"Message from partition {message.partition}, offset {message.offset}")
            print(f"Message value: {message.value}")
            no_message_count = 0  # reset counter after getting a message

        #End of Tasks
        print("Reached the end of available messages. Re-triggering producer...")
        kafkaProduer.send_task() # send tasks

    except KafkaError as e:
        print(f"Caught a KafkaException: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_and_trigger()