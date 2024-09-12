import asyncio
import threading
import time
from functools import wraps
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from collections import deque
from Data_Ingestion_Service.QueryData import query_data
import json


class ApiCircuitBreakers:
    def __init__(self, api_count, soft_limit, hard_limit, rate_limit):
        self._lock = threading.Lock()
        self._last_reset_time = time.time()
        self._current_status = "CLOSED"
        self._rate_limit = rate_limit
        self._api_count = api_count
        self._soft_limit = soft_limit
        self._hard_limit = hard_limit
        self._queue = deque()

        # Kafka Producer
        self._Producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Kafka Consumer in a separate thread
        self._consumer_thread = threading.Thread(target=self._consume_messages, daemon=True)
        self._consumer_thread.start()

    def _consume_messages(self):
        """Consume Kafka messages in a separate thread."""
        consumer = KafkaConsumer(
            'api_query',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            group_id='foo_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True
        )
        try:
            print("Consuming messages from Kafka broker...")
            for message in consumer:
                print(f"Message from partition {message.partition}, offset {message.offset}: {message.value}")
                api_query = message.value.get('API_Query')
                if api_query:
                    self._queue.append(api_query)  # Add only the API_Query to the processing queue
                    print(f"Enqueued: {api_query}")
                else:
                    print("No API_Query field found in the message.")
        except KafkaError as e:
            print(f"Caught KafkaError in consumer: {e}")
        finally:
            consumer.close()

    def enqueue_tasks(self):
        """Send API queries to Kafka."""
        try:
            for key, value in query_data.items():
                message = {"API_Query": key, 'message': f'This is the query for {value}'}
                future = self._Producer.send("api_query", value=message)
                future.get(timeout=10)  # Wait for the message to be sent
                print(f"Sent to Kafka: {message}")
        except KafkaError as e:
            print(f"Caught a KafkaException: {e}")
        finally:
            self._Producer.flush()

    def __check_and_reset_count(self):
        """Reset API count every minute."""
        current_time = time.time()
        if current_time - self._last_reset_time >= 60:
            self._api_count = 0
            self._last_reset_time = current_time

    def get_status(self):
        """Check the status of the circuit based on API count."""
        with self._lock:
            self.__check_and_reset_count()
            if self._api_count <= self._soft_limit:
                self._current_status = "CLOSED"
            elif self._soft_limit < self._api_count <= self._hard_limit:
                self._current_status = "HALF"
            else:
                self._current_status = "OPEN"
            return self._current_status

    async def process_from_queue(self):
        """Process tasks from the queue and return the API_Query."""
        if self._queue:
            api_query = self._queue.popleft()
            print(f"Processing: {api_query}")
            return api_query
        else:
            print("Queue is empty, attempting to enqueue tasks")
            await asyncio.to_thread(self.enqueue_tasks)
            return None

    async def handle_closed(self):
        """Handle request in closed state."""
        api_query = await self.process_from_queue()
        if api_query:
            return {"message": "Request processed from queue", "status": "CLOSED", "data": api_query}
        return {"message": "No request to process", "status": "CLOSED"}

    async def handle_half_open(self):
        """Handle request in half-open state with a delay."""
        await asyncio.sleep(2)
        api_query = await self.process_from_queue()
        if api_query:
            return {"message": "Request processed from queue after delay", "status": "HALF", "data": api_query}
        return {"message": "No request to process", "status": "HALF"}

    async def handle_open(self):
        """Handle request in open state."""
        return {"message": "Circuit is open, stopping requests", "status": "OPEN"}

    def __call__(self, func):
        @wraps(func)
        async def wrapped_func(*args, **kwargs):
            with self._lock:
                self._api_count += 1
                status = self.get_status()

            try:
                if status == "CLOSED":
                    return await self.handle_closed()
                elif status == "HALF":
                    return await self.handle_half_open()
                elif status == "OPEN":
                    return await self.handle_open()
            except Exception as e:
                print(f"Circuit Breaker caught an exception: {e}")
                return {"error": "Internal server error", "status": status}

        return wrapped_func

print("test")