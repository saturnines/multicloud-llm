import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from dotenv import load_dotenv
app = FastAPI()
import os

load_dotenv('DataBase.env')
from TopNCache import *

"""Secrets"""
data_ip = os.getenv('INGESTION_IP')
data_port = os.getenv('INGESTION_PORT')


# I should have made a data serializer and deserializer ugh
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


class ServiceClient:
    def __init__(self, service_url: str):
        self.service_url = service_url

    async def request(self, method: str, path: str, **kwargs) -> Optional[Dict[str, Any]]:
        # This just queries an api and returns the result
        async with httpx.AsyncClient() as client:
            try:
                response = await client.request(method, f"{self.service_url}{path}", **kwargs)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError:
                return None
            except httpx.RequestError:
                # Should never happen
                raise HTTPException(status_code=500, detail="Internal server error")


class DataIngestion(ServiceClient):
    """Call this to get one item from the Data_ingestion_Service"""

    async def query_item(self) -> Optional[Dict[str, Any]]:
        return await self.request("GET", f"/get_item")


class CacheObject:
    def __init__(self):
        self.cache = TopNCache(20)

    async def add_to_cache(self, node):
        self.cache.add(node)
        # add logging

    async def get_cache(self):
        return self.cache.get_cache()
        # add logging

    async def get_avg_cache(self):
        return self.cache.get_cache_with_averages()
        # add logging


class APIGateway:
    def __init__(self):
        self.query_service = DataIngestion(data_ip + data_port)
        self.db_service = DataIngestion(None)

    async def _get_query(self):  # Test if this works tomorrow, then start the lru service.
        """Gets from the Data Ingestion Service"""
        try:
            res = await self.query_service.query_item()
            if res:
                return res
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"}

    async def send_to_dbs(self, node):
        """Sends to DB Services, Add an event response for successful db additions."""
        pass



# Ideally Send to caching would also call an add_top_ncache too, Gotta think about interservice communication
@app.get("api/v1/get_cache_avg")
async def get_top_n_cache():
    """Return Cache Average"""
    try:
        await top_n.get_avg_cache()
    except Exception as e:
        print(f"Error as {e}")

@app.get("api/v1/get_cache")
async def get_top_n_cache():
    """Return Top-N Cache"""
    try:
        await top_n.get_cache()
    except Exception as e:
        print(f"Error as {e}")

@app.post("api/v1/cache_add")
async def get_top_n_cache(node):
    """add to cache"""
    try:
        await top_n.add_to_cache(node)
    except Exception as e:
        print(f"Error as {e}")


if __name__ == "__main__":
    top_n = CacheObject()
    uvicorn.run(app, host="0.0.0.0", port=8002)