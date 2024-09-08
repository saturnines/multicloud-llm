import Data_Ingestion_Service.kafkaProduer
import Data_Ingestion_Service.kafkaConsumer
from kafka.errors import KafkaError
import service_decorators.service_breakers_deco
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import requests

# Functions for Kafka (query and fill data)
producer_fill_data = Data_Ingestion_Service.kafkaProduer.query_data()
retrigger_data = Data_Ingestion_Service.kafkaConsumer.consume_and_trigger()
next_data = Data_Ingestion_Service.kafkaConsumer.get_next_message()


# Start of Data Query

data_api = FastAPI()

# Fill up The Data
producer_fill_data()


def kafka_data_grabber():
    try:
        next_query = next_data()

        if next_data is None:
            retrigger_data()
        return next_query
    except KafkaError as e:
        print(f"KafkaException {e}")

# Negative Cache
neg_cache = {}

class Metrics(BaseModel):
    """Unsure if it some querys might be missing"""
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

class ApiResult(BaseModel):
    Signal: str
    metrics: Metrics  # Nested dict.


@data_api.get("/get_next")
@service_decorators.service_breakers_deco.ApiCircuitBreakers(0, 55, 90, 100)
def query_data():
    data = None

    # Fetch data from Kafka
    while data is None or data in neg_cache:
        data = kafka_data_grabber()

    try:
        # Query external API
        response = requests.get(f"https://api.kevinsapi.net/items/?search_term={data}")

        if response.status_code == 200:
            api_response = response.json()
            # Validate response
            result = ApiResult(**api_response)
            return result
        else:
            neg_cache[data] = True  # cache the negative result
            return {"error": "No data found for query", "query": data}

    except Exception as e:
        print(f"API call failed: {e}")
        return {"error": "Failed to fetch data"}








# https://api.kevinsapi.net/items/?search_term=wheat Api call example
