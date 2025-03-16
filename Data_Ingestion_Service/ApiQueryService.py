import json
import os
import asyncio
import time
import httpx
from kafka import KafkaProducer
from pydantic import BaseModel, ValidationError
from typing import Optional
from Data_Ingestion_Service.service_breakers_deco import ApiCircuitBreakers
from Data_Ingestion_Service.DataIngestionLogConfig import configure_logging
from dotenv import load_dotenv
import random

load_dotenv('DataEnv.env')
logger = configure_logging('Data_Ingestion_Service')

kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

api_query_topic = 'api_query'
database_operations_topic = 'database_operations'



class Metrics(BaseModel):
    profitability: Optional[float] = None
    volatility: Optional[float] = None
    liquidity: Optional[float] = None
    price_momentum: Optional[float] = None
    relative_volume: Optional[float] = None
    spread: Optional[float] = None
    price_stability: Optional[float] = None
    historical_buy_comparison: Optional[float] = None
    historical_sell_comparison: Optional[float] = None
    medium_sell: Optional[float] = None
    medium_buy: Optional[float] = None
    possible_profit: Optional[float] = None
    current_price: Optional[float] = None
    instant_sell: Optional[float] = None
    search_Query: Optional[str] = None


class ItemResponse(BaseModel):
    signal: Optional[str] = None
    metrics: Metrics


class KafkaEventProcessor:
    def __init__(self):
        logger.info("Initializing Kafka")
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Successfully started Kafka producer")

        self.api_breaker = ApiCircuitBreakers(
            api_count=0,
            soft_limit=55,
            hard_limit=90,
            rate_limit=100
        )
        logger.info("Circuit breaker initialized")

        self.negative_cache = set([])
        self.retry_counts = {}
        self.last_retry_time = {}

    def reset_retries(self, search_term):
        """Reset retry count and time for a search term"""
        if search_term in self.retry_counts:
            del self.retry_counts[search_term]
        if search_term in self.last_retry_time:
            del self.last_retry_time[search_term]
        logger.debug(f"Reset retries for search term: {search_term}")

    async def process_item(self, search_term, max_retries=3, base_delay=1):
        retries = self.retry_counts.get(search_term, 0)

        try:
            if search_term in self.negative_cache:
                logger.info(f"Skipping cached negative result for: {search_term}")
                self.reset_retries(search_term)
                return None

            if retries > 0:
                delay = base_delay * (2 ** (retries - 1))
                logger.info(f"Retry attempt {retries}/{max_retries} for {search_term}, delay: {delay}s")
                await asyncio.sleep(delay)

            if random.random() < 0.01:
                current_time = time.strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"[{current_time}] Sample negative cache size: {len(self.negative_cache)}")

            # rate limit
            await asyncio.sleep(4)

            logger.info(f"Making API request for: {search_term}")
            url = f"https://api.kevinsapi.net/items/?search_term={search_term}"

            async with httpx.AsyncClient() as client:
                response = await client.get(url)

                if response.status_code == 200:
                    logger.info(f"Successful API response for: {search_term}")
                    data = response.json()
                    signal = data.get("Signal")
                    metrics_data = data.get("metrics", {})
                    metrics_data['search_query'] = search_term

                    try:
                        item = ItemResponse(signal=signal, metrics=Metrics(**metrics_data))
                        self.producer.send(api_query_topic, value=item.dict())
                        self.producer.flush()
                        logger.info(f"Successfully processed and sent data to Kafka for: {search_term}")
                        self.reset_retries(search_term)
                        return True

                    except ValidationError as e:
                        logger.error(f"Validation error for {search_term}: {str(e)}")
                        self.negative_cache.add(search_term)
                        self.reset_retries(search_term)
                        return None

                elif response.status_code == 404:
                    logger.warning(f"Not found error for {search_term}, adding to negative cache")
                    self.negative_cache.add(search_term)
                    self.reset_retries(search_term)
                    return None

                elif response.status_code in {500, 503}:
                    logger.error(
                        f"Server error {response.status_code} for {search_term}, attempt {retries + 1}/{max_retries}")
                    if retries < max_retries:
                        self.retry_counts[search_term] = retries + 1
                        self.last_retry_time[search_term] = time.time()
                        return await self.process_item(search_term, max_retries, base_delay)
                    else:
                        logger.error(f"Max retries reached for {search_term}")
                        self.reset_retries(search_term)
                        return None

            return None

        except Exception as e:
            logger.error(f"Unexpected error processing {search_term}: {str(e)}, attempt {retries + 1}/{max_retries}")
            if retries < max_retries:
                self.retry_counts[search_term] = retries + 1
                self.last_retry_time[search_term] = time.time()
                return await self.process_item(search_term, max_retries, base_delay)
            else:
                logger.error(f"Max retries reached after unexpected error for {search_term}")
                self.reset_retries(search_term)
                return None

    async def run(self):
        logger.info("Starting KafkaEventProcessor service...")
        try:

            while True:

                search_term = await self.api_breaker.process_from_queue()


                if not search_term:
                    self.api_breaker.enqueue_tasks()

                    await asyncio.sleep(5)
                    continue


                result = await self.process_item(search_term)


        except Exception as e:

            logger.error(f"Fatal error in event processor: {str(e)}")
        finally:
            logger.info("Shutting down KafkaEventProcessor")
            self.producer.close()

if __name__ == "__main__":
    logger.info("Initializing Data Ingestion Service")
    print("running kafka processer")
    processor = KafkaEventProcessor()
    print("running  processor")
    asyncio.run(processor.run())
