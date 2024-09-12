import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any
import asyncio
from Data_Ingestion_Service.service_breakers_deco import ApiCircuitBreakers

# Initialize FastAPI app
app = FastAPI()


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

# main response model
class ItemResponse(BaseModel):
    signal: Optional[str] = None
    metrics: Metrics


api_breaker = ApiCircuitBreakers(api_count=0, soft_limit=55, hard_limit=90, rate_limit=100)
async def api_helper():
    item = await api_breaker.process_from_queue()
    if not item:
        api_breaker.enqueue_tasks()
        await asyncio.sleep(5)
    if item:
        return item

negative_cache = set([])


@app.get("/get_item", response_model=ItemResponse)
async def get_item():
    """Internal endpoint to get an item with a 5-second delay"""
    res = await api_helper() # skip the null block

    try:
        res = await api_helper()
        if res is None:
            return {"error": "No search term provided, query."}

        # 3 second delay because is faster than the actual api.
        await asyncio.sleep(5)

        # Assuming `res` is the search term
        url = f"https://api.kevinsapi.net/items/?search_term={res}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url)

            if response.status_code == 200:
                data = response.json()

                # get both signal and metrics response
                signal = data.get("Signal")
                metrics_data = data.get("metrics", {})

                # Return the response with pydantic
                return ItemResponse(signal=signal, metrics=Metrics(**metrics_data))

            elif response.status_code == 404:
                return {"error": "Item not found"}

            elif response.status_code in {500, 503}:
                return {"error": "Server error, please try again later"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)