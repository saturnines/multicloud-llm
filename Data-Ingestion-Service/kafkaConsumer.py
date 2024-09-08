from kafka import KafkaConsumer
import json

from kafka.errors import KafkaError

consumer = KafkaConsumer(
    'api_query',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='foo_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
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
