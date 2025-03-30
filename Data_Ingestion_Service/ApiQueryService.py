import json
import os
import asyncio
import sys
import time
import httpx
from kafka import KafkaProducer
from pydantic import BaseModel, ValidationError
from typing import Optional
from Data_Ingestion_Service.service_breakers_deco import ApiCircuitBreakers
from Data_Ingestion_Service.DataIngestionLogConfig import configure_logging
from dotenv import load_dotenv
import random
from common.promMetrics import prometheus_monitor, start_prometheus_server


load_dotenv('DataEnv.env')
logger = configure_logging('Data_Ingestion_Service')
start_prometheus_server(service_name="data_ingestion_service", port=8010)
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
    search_query: Optional[str] = None


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


    @prometheus_monitor(service_name="data_ingestion_service")
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
            await asyncio.sleep(3)

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
                try:
                    search_term = await self.api_breaker.process_from_queue()

                    if not search_term:
                        try:
                            self.api_breaker.enqueue_tasks()
                        except Exception as enqueue_error:
                            logger.error(f"Error enqueueing tasks: {str(enqueue_error)}")

                        await asyncio.sleep(5)
                        continue

                    try:
                        result = await self.process_item(search_term)
                        if result:
                            logger.info(f"Successfully processed item: {search_term}")
                        else:
                            logger.warning(f"Failed to process item: {search_term}")
                    except Exception as process_error:
                        logger.error(f"Error processing item {search_term}: {str(process_error)}")

                except Exception as e:
                    logger.error(f"Error in main loop: {str(e)}")
                    await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Fatal error in event processor: {str(e)}")
        finally:
            logger.info("Shutting down KafkaEventProcessor")
            self.producer.close()


def ensure_kafka_ready(bootstrap_servers, max_attempts=30, delay=2):
    """Make sure Kafka is online"""
    import time
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable, NodeNotReadyError

    for attempt in range(max_attempts):
        try:
            #test to see if kafka works
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.flush()
            producer.close()

            logger.info(f"Kafka connection successful on attempt {attempt + 1}")
            return True

        except (NoBrokersAvailable, NodeNotReadyError) as e:
            logger.warning(f"Kafka not ready (attempt {attempt + 1}/{max_attempts}): {e}")
            time.sleep(delay)
        except Exception as e:
            logger.warning(f"Unexpected error connecting to Kafka (attempt {attempt + 1}/{max_attempts}): {e}")
            time.sleep(delay)

    logger.error("Failed to connect to Kafka after multiple attempts")
    return False


if __name__ == "__main__":
    logger.info("Initializing Data Ingestion Service")

    # Ensure Kafka is ready before proceeding
    if not ensure_kafka_ready(kafka_bootstrap_servers):
        logger.error("Kafka not ready, aborting startup")
        sys.exit(1)

    print("running kafka processer")
    processor = KafkaEventProcessor()
    print("running processor")
    asyncio.run(processor.run())