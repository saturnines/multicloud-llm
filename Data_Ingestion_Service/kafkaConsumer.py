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
    enable_auto_commit=True  # offset for committing
)


def get_next_message():
    try:
        print("Consuming messages from broker")
        for message in consumer:
            print(f"Message from partition {message.partition}, {message.offset}")
            print(f"Message value:{message.value}")
            return message.value # This is the actual query
    except KafkaError as e:
        print(f"Caught a KafkaException:{e}")
    finally:
        consumer.close()


def consume_and_trigger():
    """Consumes messages from Kafka and triggers send_task to fill the data (This is an easy way to update data too)"""
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