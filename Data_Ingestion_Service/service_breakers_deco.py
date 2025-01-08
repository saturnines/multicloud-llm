import asyncio
import threading
import time
from functools import wraps

import aiohttp
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from collections import deque
from Data_Ingestion_Service.QueryData import query_data
import json
import os
from dotenv import load_dotenv

import os

load_dotenv('DataEnv.env')
from Data_Ingestion_Service.DataIngestionLogConfig import configure_logging

logger = configure_logging('Circuit_Breaker')


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

        kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

        self._Producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # kafka consumer in separate thread ( this is prolly not threadsafe but I don't know enough about threading lol)
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
            logger.info("Starting Kafka message consumption")
            for message in consumer:
                logger.info("Message received from Kafka", extra={
                    'partition': message.partition,
                    'offset': message.offset,
                    'value': message.value
                })
                api_query = message.value.get('API_Query')
                if api_query:
                    self._queue.append(api_query)
                    logger.info("Query enqueued", extra={
                        'query': api_query,
                        'queue_size': len(self._queue)
                    })
                else:
                    logger.warning("No API_Query field in message", extra={
                        'message_value': message.value
                    })
        except KafkaError as e:
            logger.error("Kafka consumer error", extra={
                'error': str(e)
            })
        finally:
            consumer.close()
            logger.info("Kafka consumer closed")

    def enqueue_tasks(self):
        """Send API queries to Kafka."""
        try:
            for key, value in query_data.items():
                message = {"API_Query": key, 'message': f'This is the query for {value}'}
                future = self._Producer.send("api_query", value=message)
                future.get(timeout=10)  # Wait for the message to be sent
                logger.info(f"Sent to Kafka: {message}")
        except KafkaError as e:
            logger.error(f"Caught a KafkaException: {e}")
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
            logger.info(f"Processing: {api_query}")
            return api_query
        else:
            logger.info("Queue is empty, attempting to enqueue tasks")
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
                logger.error("Circuit Breaker exception", extra={
                    'error': str(e),
                    'status': status
                })
                return {"error": "Internal server error", "status": status}

        return wrapped_func