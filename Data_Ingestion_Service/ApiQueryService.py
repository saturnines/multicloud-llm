import json
import os
import asyncio
import time
import httpx
from kafka import KafkaProducer
from pydantic import BaseModel, ValidationError
from typing import Optional
from Data_Ingestion_Service.service_breakers_deco import ApiCircuitBreakers


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
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.api_breaker = ApiCircuitBreakers(
            api_count=0,
            soft_limit=55,
            hard_limit=90,
            rate_limit=100
        )

        self.negative_cache = set([])
        self.retry_counts = {}
        self.last_retry_time = {}

    def reset_retries(self, search_term):
        """Reset retry count and time for a search term"""
        if search_term in self.retry_counts:
            del self.retry_counts[search_term]
        if search_term in self.last_retry_time:
            del self.last_retry_time[search_term]

    async def process_item(self, search_term, max_retries=3, base_delay=1):
        retries = self.retry_counts.get(search_term, 0)

        try:
            if search_term in self.negative_cache:
                self.reset_retries(search_term)
                return None


            if retries > 0:
                delay = base_delay * (2 ** (retries - 1))
                print(f"Retry {retries} for {search_term}, waiting {delay} seconds")
                await asyncio.sleep(delay)

            # Regular rate limit delay
            await asyncio.sleep(4)

            url = f"https://api.kevinsapi.net/items/?search_term={search_term}"

            async with httpx.AsyncClient() as client:
                response = await client.get(url)

                if response.status_code == 200:
                    data = response.json()
                    signal = data.get("Signal")
                    metrics_data = data.get("metrics", {})
                    metrics_data['search_Query'] = search_term

                    try:
                        item = ItemResponse(signal=signal, metrics=Metrics(**metrics_data))
                        self.producer.send(api_query_topic, value=item.dict())
                        self.producer.flush()
                        print(f"Processed and sent data for search term: {search_term}")
                        self.reset_retries(search_term)  # Reset on success
                        return True

                    except ValidationError as e:
                        print(f"Validation error for {search_term}: {e}")
                        self.negative_cache.add(search_term)
                        self.reset_retries(search_term)
                        return None

                elif response.status_code == 404:
                    print(f"Not found error for {search_term}")
                    self.negative_cache.add(search_term)
                    self.reset_retries(search_term)
                    return None

                elif response.status_code in {500, 503}:
                    print(f"Server error {response.status_code} for {search_term}, attempt {retries + 1}/{max_retries}")
                    if retries < max_retries:
                        self.retry_counts[search_term] = retries + 1
                        self.last_retry_time[search_term] = time.time()
                        return await self.process_item(search_term, max_retries, base_delay)
                    else:
                        print(f"Max retries reached for {search_term}")
                        self.reset_retries(search_term)
                        return None

            return None

        except Exception as e:
            print(f"Error processing item: {e}, attempt {retries + 1}/{max_retries}")
            if retries < max_retries:
                self.retry_counts[search_term] = retries + 1
                self.last_retry_time[search_term] = time.time()
                return await self.process_item(search_term, max_retries, base_delay)
            else:
                print(f"Max retries reached for {search_term}")
                self.reset_retries(search_term)
                return None



    async def run(self):
        print("Starting to process items...")
        try:
            while True:
                search_term = await self.api_breaker.process_from_queue()
                if not search_term:
                    self.api_breaker.enqueue_tasks()
                    await asyncio.sleep(5)
                    continue

                await self.process_item(search_term)

        except Exception as e:
            print(f"Error in event processor: {e}")
        finally:
            self.producer.close()


if __name__ == "__main__":
    processor = KafkaEventProcessor()
    asyncio.run(processor.run())