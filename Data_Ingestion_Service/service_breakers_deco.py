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
        bootstrap_servers='localhost:9092', # Start Connection Maybe change it?
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        # Kafka Consumer
        self._consumer = KafkaConsumer(
            'api_query',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            group_id='foo_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True  # offset for committing
        )

    def enqueue_tasks(self):
        """Grab from QueryData"""
        try:
            for key, value in query_data.items():
                message = {"API_Query": value, 'message': f'This is the query for {key}'}
                future = self._Producer.send("api_query", value=message)
                # Wait for the message to be sent
                future.get(timeout=10)
                print(f"Sent: {message}")

        except KafkaError as e:
            print(f"Caught a KafkaException: {e}")
        finally:
            self._Producer.flush()

    def send_to_queue(self):
        try:
            print("Consuming messages from broker")
            for message in self._consumer:
                print(f"Message from partition {message.partition}, {message.offset}")
                print(f"Message value:{message.value}")
                return message.value  # This is the actual query
        except KafkaError as e:
            print(f"Caught a KafkaException:{e}")
        finally:
            self._consumer.close()

    def __check_and_reset_count(self):
        current_time = time.time()
        if current_time - self._last_reset_time >= 60:
            self._api_count = 0
            self._last_reset_time = current_time

    def get_status(self):
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
        if self._queue:
            return self._queue.popleft()
        else:
            await asyncio.to_thread(self.enqueue_tasks)
        return None

    async def handle_closed(self):
        # In closed state, process from queue if available, otherwise process the request directly
        queued_request = await self.process_from_queue()
        if queued_request:
            return await queued_request


    async def handle_half_open(self):
        # Send a delay of 2 seconds to process api
        await asyncio.sleep(2)
        await self.process_from_queue()
        return {"message": "Circuit is half-open, request queued", "status": "HALF"}

    async def handle_open(self):
        return {"message": "Circuit is open, stopping requests", "status": "OPEN"}


    def __call__(self, func):
        @wraps(func)
        async def wrapped_func():
            # Ensure the queue is populated
            if not self._queue:
                self._queue.append(self.enqueue_tasks())

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


