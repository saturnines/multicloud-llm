import uvicorn
from fastapi import FastAPI, HTTPException
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
import json
import threading
app = FastAPI()
import os

from DatabaseLogConfig import configure_logging
logger = configure_logging('DB_Gateway')
load_dotenv('DataBase.env')
from TopNCache import *
from common.promMetrics import prometheus_monitor, start_prometheus_server


start_prometheus_server(service_name="DB_Gateway", port=8010)
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
kafka_topic = os.getenv('KAFKA_TOPIC', 'api_query')  # Topic from data ingestion


class Metrics(BaseModel):
    """Model for expected API result from Data Ingestion"""
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

class ItemResponse(BaseModel):
    signal: Optional[str] = None
    metrics: Metrics

class APIGateway:
    def __init__(self):
        logger.info("Initializing API Gateway", extra={
            'kafka_topic': kafka_topic,
            'bootstrap_servers': kafka_bootstrap_servers
        })

        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            group_id='gateway_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.consumer_thread = threading.Thread(target=self._consume_messages, daemon=True)
        self.consumer_thread.start()

    @prometheus_monitor(service_name="DB_gateway")
    def _consume_messages(self):
        try:
            logger.info("Starting to consume messages from data ingestion...")
            for message in self.consumer:
                try:
                    data = message.value
                    logger.info(f"Received raw message: {data}")

                    # skip messages that don't have sig and metrics
                    if not (data and isinstance(data, dict) and 'signal' in data and 'metrics' in data):
                        logger.warning("Invalid message format", extra={
                            'received_data': data
                        })
                        continue


                    node = HeapNode(
                        signal=data.get('signal'),
                        profitability=data['metrics'].get('profitability'),
                        volatility=data['metrics'].get('volatility'),
                        liquidity=data['metrics'].get('liquidity'),
                        price_stability=data['metrics'].get('price_stability'),
                        relative_volume=data['metrics'].get('relative_volume'),
                        possible_profit=data['metrics'].get('possible_profit'),
                        current_price=data['metrics'].get('current_price'),
                        search_query=data['metrics'].get('search_query')
                    )

                    top_n.cache.add(node)
                    self.producer.send('database_operations', value=data)
                    logger.info(f"Processed data: {data}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.consumer.close()

class CacheObject:
    def __init__(self):
        self.cache = TopNCache(20)

    async def get_cache(self):
        return self.cache.get_cache()

    async def get_avg_cache(self):
        return self.cache.get_cache_with_averages()


top_n = CacheObject()
gateway = APIGateway()

@prometheus_monitor(service_name="DB_Gateway")
@app.get("/api/v1/get_cache")
async def get_top_n_cache():
    """Return Top-N Cache"""
    try:
        return await top_n.get_cache()
    except Exception as e:
        logger.error("Cache retrieval error", extra={
            'error': str(e),
            'error_type': type(e).__name__
        })
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)