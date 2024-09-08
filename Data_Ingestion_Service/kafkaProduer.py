from kafka import KafkaProducer
from kafka.errors import KafkaError
from QueryData import query_data
import json


# Start producer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092', # Start Connection Maybe change it?
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_task():
    try:
        for key, value in query_data.items():
            message = {"API_Query": value, 'message': f'This is the query for {key}'}
            future = producer.send("api_query", value=message)
            # Wait for the message to be sent
            future.get(timeout=10)
            print(f"Sent: {message}")

    except KafkaError as e:
        print(f"Caught a KafkaException: {e}")
        # Replace print with logging
    finally:
        producer.flush()
        producer.close()




if __name__ == "__main__":
    send_task()