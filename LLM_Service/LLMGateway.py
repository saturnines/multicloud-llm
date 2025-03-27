import requests
import json
import logging
import os
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
import httpx
import re
from common.promMetrics import prometheus_monitor, start_prometheus_server

# Logging for efk
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("LLMGateway")

app = FastAPI()

#prom
start_prometheus_server(service_name="LLM_Service", port=8013)


class QueryRequest(BaseModel):
    query: str


class LLMEnvironment:
    def __init__(self):
        load_dotenv('LLM.env')
        self.fuzzy_url = os.getenv('fuzzy_url')
        self.db_url = os.getenv('db_url')
        self.llm_url = os.getenv('llm_url')
        self.api_key = os.getenv('api_key')

        if not self.api_key:
            logger.warning("API key not found in environment variables. API calls may fail.")


class OpenWebUIClient:
    """WebUi Client for function calling and api calling."""

    def __init__(self, base_url, api_key=None):
        self.base_url = base_url
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}" if api_key else None
        }
        logger.info(f"Initialized OpenWebUIClient with base URL: {base_url}")

    @prometheus_monitor(service_name="LLM_Service")
    def analyze_market_data(self, query: str, market_data: Dict[str, Any], item_name: str) -> Dict[str, Any]:
        """Analyze market data using function calling"""

        # Different functions for function calling
        functions = [
            {
                "name": "analyze_market_trend",
                "description": "Analyze market trends and provide trading recommendations",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "recommendation": {
                            "type": "string",
                            "enum": ["BUY", "SELL", "WATCH", "AVOID"],
                            "description": "Trading recommendation based on market analysis"
                        },
                        "price_trend": {
                            "type": "string",
                            "enum": ["RISING", "FALLING", "STABLE", "VOLATILE"],
                            "description": "Current price trend in the market"
                        },
                        "profit_potential": {
                            "type": "number",
                            "description": "Estimated profit potential percentage"
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Confidence level in the recommendation (0-100)"
                        },
                        "reasoning": {
                            "type": "string",
                            "description": "Detailed explanation for the recommendation"
                        },
                        "risk_level": {
                            "type": "string",
                            "enum": ["LOW", "MEDIUM", "HIGH"],
                            "description": "Risk assessment based on market volatility"
                        }
                    },
                    "required": ["recommendation", "price_trend", "profit_potential", "confidence", "reasoning",
                                 "risk_level"]
                }
            }
        ]

        # Prompt  (Might need to adjust this)
        system_instruction = f"""
        You are a trading analysis assistant for Hypixel SkyBlock.

        Analyze the market data for {item_name} and provide detailed trading recommendations.
        Consider all metrics including profitability, volatility, liquidity, price stability, and more.

        Focus on answering the user's query: "{query}"

        When using functions, be precise and data-driven in your analysis.

        Keep your tone friendly, informative, and use gaming terminology appropriate for Hypixel SkyBlock players.
        """

        # Message to send via API
        messages = [
            {"role": "system", "content": system_instruction},
            {"role": "user",
             "content": f"Here is the market data for {item_name}:\n{json.dumps(market_data, indent=2)}\n\nPlease analyze this data and provide trading recommendations."}
        ]

        try:
            # API call with function calling
            endpoint = f"{self.base_url}/api/chat/completions"

            payload = {
                "model": "deepseek-r1:7b",
                "messages": messages,
                "functions": functions,
                "temperature": 0.3
            }

            response = requests.post(endpoint, headers=self.headers, json=payload)

            if response.status_code == 200:
                result = response.json()

                # process request
                if "choices" in result and len(result["choices"]) > 0:
                    message = result["choices"][0]["message"]

                    # check if function was called
                    if "function_call" in message:
                        function_call = message["function_call"]
                        function_name = function_call["name"]

                        try:
                            function_args = json.loads(function_call["arguments"])

                            if function_name == "analyze_market_trend":
                                return {
                                    "type": "market_analysis",
                                    "item": item_name,
                                    "recommendation": function_args["recommendation"],
                                    "price_trend": function_args["price_trend"],
                                    "profit_potential": function_args["profit_potential"],
                                    "confidence": function_args["confidence"],
                                    "reasoning": function_args["reasoning"],
                                    "risk_level": function_args["risk_level"],
                                    "original_query": query
                                }
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.error(f"Error parsing function arguments: {e}")

                    # back up with no function was called (might need to log this)
                    return {
                        "type": "text_analysis",
                        "item": item_name,
                        "content": message["content"],
                        "original_query": query
                    }

            logger.error(f"API error: {response.status_code} - {response.text}")
            return {"error": f"API error: {response.status_code}", "item": item_name}

        except Exception as e:
            logger.error(f"Exception in analyze_market_data: {e}")
            return {"error": str(e), "item": item_name}

    def suggest_related_items(self, item_name: str, market_data: Dict[str, Any]) -> List[str]:
        """Suggest  items """
        messages = [
            {"role": "system",
             "content": "You are a knowledgeable Hypixel SkyBlock assistant. Suggest related items based on the user's interest."},
            {"role": "user",
             "content": f"Based on interest in {item_name}, suggest 3-5 related items that might also be worth trading."}
        ]

        try:
            endpoint = f"{self.base_url}/api/chat/completions"

            payload = {
                "model": "deepseek-r1:7b",
                "messages": messages,
                "temperature": 0.5
            }

            response = requests.post(endpoint, headers=self.headers, json=payload)

            if response.status_code == 200:
                result = response.json()
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0]["message"]["content"]
                    # use regex to find

                    items = re.findall(r'- ([A-Za-z ]+)', content)
                    return items[:5]  # Return at most 5 items

            return []
        except Exception as e:
            logger.error(f"Error suggesting related items: {e}")
            return []


class LLMGateway:
    def __init__(self):
        env = LLMEnvironment()
        self.fuzzy_url = env.fuzzy_url
        self.db_url = env.db_url
        self.llm_url = env.llm_url
        self.api_key = env.api_key

        # webui client with function calling
        self.llm_client = OpenWebUIClient(base_url="http://localhost:3000", api_key=self.api_key)
        logger.info("Initialized LLMGateway")

    @prometheus_monitor(service_name="LLM_Service")
    async def get_item_match(self, query: str):
        """Gets fuzzy string parsed match from FuzzySearch.py"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.fuzzy_url}/fuzzy?query={query}")
                if response.status_code == 404:
                    logger.info(f"No item match found for query: '{query}'")
                    return None
                return response.json()["match"]
        except Exception as e:
            logger.error(f"Error getting item match: {e}")
            return None

    @prometheus_monitor(service_name="LLM_Service")
    async def get_market_data(self, item_id: str):
        """Gets the market data from database service"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.db_url}/api/v1/read_data/?data={item_id}")
                return response.json()
        except Exception as e:
            logger.error(f"Error getting market data: {e}")
            return {}

    @prometheus_monitor(service_name="LLM_Service")
    async def _get_top_items_from_cache(self) -> List[Dict[str, Any]]:
        """Get top items from the TopNCache"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.db_url}/api/v1/get_cache")

                if response.status_code == 200:
                    # parse json data
                    cache_data = json.loads(response.text)

                    results = []
                    for item in cache_data:
                        if isinstance(item, dict) and "name" in item:
                            item_name = item.get("name", "Unknown Item")

                            # generate analysis for this item using function calling
                            analysis = self.llm_client.analyze_market_data(
                                f"Analyze {item_name}",
                                item,
                                item_name
                            )

                            results.append({
                                "item": item_name,
                                "market_data": item,
                                "analysis": analysis
                            })

                    return results

                logger.warning("Failed to get data from TopNCache")
                return []
        except Exception as e:
            logger.error(f"Error getting top items from cache: {e}")
            return []

    @prometheus_monitor(service_name="LLM_Service")
    async def get_random_recommendations(self, count: int = 5) -> List[Dict[str, Any]]:
        """Get random item recommendations with analysis"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.db_url}/api/v1/random_data/")

                if response.status_code == 200:
                    items = response.json()

                    results = []
                    for item in items[:count]:
                        item_name = item.get("search_query", "Unknown Item")

                        # get analysis of this item
                        analysis = self.llm_client.analyze_market_data(
                            f"Analyze {item_name}",
                            item,
                            item_name
                        )

                        results.append({
                            "item": item_name,
                            "market_data": item,
                            "analysis": analysis
                        })

                    return results
                return []
        except Exception as e:
            logger.error(f"Error getting random recommendations: {e}")
            return []

    @prometheus_monitor(service_name="LLM_Service")
    async def process_query(self, query: str):
        """Process a user query"""
        try:

            item_name = await self.get_item_match(query)

            if item_name:
                logger.info(f"Found matching item: '{item_name}' for query: '{query}'")

                # just grabs market data -> analysis
                market_data = await self.get_market_data(item_name)

                analysis = self.llm_client.analyze_market_data(query, market_data, item_name)

                related_items = self.llm_client.suggest_related_items(item_name, market_data)

                # Step 5: Format response
                return {
                    "success": True,
                    "query": query,
                    "type": "specific_item_analysis",
                    "item": {
                        "name": item_name,
                        "id": item_name  # Using name as ID for now
                    },
                    "analysis": analysis,
                    "market_data": market_data,
                    "related_items": related_items
                }

            # if there isnt anything we just get the top item from the top n cache from db service
            logger.info(f"No specific item found for query: '{query}'. Trying TopNCache.")
            top_items = await self._get_top_items_from_cache()

            # If we got items from the cache just return that
            if top_items and len(top_items) > 0:
                return {
                    "success": True,
                    "query": query,
                    "type": "top_items_recommendation",
                    "message": "I couldn't find that specific item. Here are the current top trading opportunities:",
                    "recommendations": top_items
                }

            # If cache failed we just use get random five
            logger.info("TopNCache failed or empty. Falling back to random recommendations.")
            random_items = await self.get_random_recommendations(5)

            return {
                "success": True,
                "query": query,
                "type": "random_recommendation",
                "message": "I couldn't find that specific item or get top items. Here are some random items that might interest you:",
                "recommendations": random_items
            }

        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return {
                "success": False,
                "error": str(e),
                "query": query
            }


gateway = LLMGateway()


@app.post("/process")
async def process_query(request: QueryRequest):
    return await gateway.process_query(request.query)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8004)
