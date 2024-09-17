import httpx
from fastapi import FastAPI, HTTPException
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from dotenv import load_dotenv

import os
load_dotenv('DataBase.env')


"""Secrets"""
data_ip = os.getenv('INGESTION_IP')
data_port = os.getenv('INGESTION_PORT')

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

class APIGateway:
    def __init__(self):
        self.query_service = DataIngestion(data_ip + data_port)
        self.caching_service = DataIngestion(None)
        self.db_service = DataIngestion(None)


    async def _get_query(self): # Test if this works tomorrow, then start the lru service.
        """Gets from the Data Ingestion Service"""
        try:
            res = await self.query_service.query_item()
            if res:
                return res
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"}


    async def send_to_caching(self):
        """Sends to LRU cache service"""
        """Expected API RESULT
        
        """
        pass


    async def send_to_db(self):
        """Sends to DB Service"""
        pass



# Example api result
"""
{
  "Signal": "Buy",
  "metrics": {
    "profitability": -98.3969501368968,
    "volatility": -31901.683860371424,
    "liquidity": 124.69620609103104,
    "price_momentum": 30.17405152275776,
    "relative_volume": 1572138.314079518,
    "spread": -241.5227965997328,
    "price_stability": 4711.62251119729,
    "historical_buy_comparison": 4132.027661912025,
    "historical_sell_comparison": -1.629805220883751,
    "medium_sell": 4.0,
    "medium_buy": 5.8,
    "possible_profit": 0.19500000000000028,
    "current_price": 124.69620609103104,
    "instant_sell": 4.126199999999992
  }
}

"""

