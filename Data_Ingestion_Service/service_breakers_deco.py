import threading
import time
from functools import wraps
from fastapi import Request
import asyncio
from collections import deque

from Data_Ingestion_Service import kafkaConsumer, kafkaProduer

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

    async def produce_message(self, message):
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, kafkaProduer.send_task)
            print(f"Message produced: {message}")
        except Exception as e:
            print(f"Failed to produce message: {e}")
            raise

    async def consume_and_queue_message(self):
        try:
            loop = asyncio.get_running_loop()
            message = await loop.run_in_executor(None, kafkaConsumer.get_next_message)
            if message:
                self._queue.append(message)
                print(f"Message consumed and queued: {message}")
            return message
        except Exception as e:
            print(f"Failed to consume message: {e}")
            raise

    async def process_from_queue(self):
        if self._queue:
            message = self._queue.popleft()
            print(f"Processed from queue: {message}")
            return message
        print("Queue is empty")
        return None

    async def handle_closed(self, func, request: Request):
        queued_request = await self.process_from_queue()
        if queued_request:
            return await func(queued_request)
        return await func(request)

    async def handle_half_open(self, request: Request):
        await self.produce_message(request)
        return {"message": "Circuit is half-open, request queued", "status": "HALF"}

    async def handle_open(self, request: Request):
        await self.produce_message(request)
        return {"message": "Circuit is open, request queued", "status": "OPEN"}

    def __call__(self, func):
        @wraps(func)
        async def wrapped_func(request: Request):
            if not self._queue:
                await self.consume_and_queue_message()

            with self._lock:
                self._api_count += 1
                status = self.get_status()

            try:
                if status == "CLOSED":
                    return await self.handle_closed(func, request)
                elif status == "HALF":
                    return await self.handle_half_open(request)
                elif status == "OPEN":
                    return await self.handle_open(request)
            except Exception as e:
                print(f"Circuit Breaker caught an exception: {e}")
                return {"error": "Internal server error", "status": status}

        return wrapped_func

