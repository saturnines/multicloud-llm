import os
from asyncio import Lock
from decimal import Decimal
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from dotenv import load_dotenv
import asyncpg
from contextlib import asynccontextmanager

load_dotenv('DataBase.env')

from DatabaseLogConfig import configure_logging
logger = configure_logging('SQL_Operations')


# Variables
db_host = os.getenv('HOST')
db_name = os.getenv('DBNAME')
db_user = os.getenv("USER")
db_password = os.getenv("PASSWORD")
db_port = os.getenv("DB_PORT")

class Metrics(BaseModel):
    profitability: Optional[float]
    volatility: Optional[float]
    liquidity: Optional[float]
    price_momentum: Optional[float]
    relative_volume: Optional[float]
    spread: Optional[float]
    price_stability: Optional[float]
    historical_buy_comparison: Optional[float]
    historical_sell_comparison: Optional[float]
    medium_sell: Optional[float]
    medium_buy: Optional[float]
    possible_profit: Optional[float]
    current_price: Optional[float]
    instant_sell: Optional[float]
    search_Query: Optional[str] = None
class SignalData(BaseModel):
    signal: str
    metrics: Metrics

class DataBaseCreator:
    """Create DataBase"""

    def __init__(self):
        self.db_host = db_host
        self.db_user = db_user
        self.db_name = db_name
        self.db_password = db_password
        self.db_port = db_port

    async def create_db(self):
        """Creates the db with schema"""
        conn = await asyncpg.connect(host=self.db_host, database=self.db_name, user=self.db_user, password=self.db_password, port=self.db_port)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS data_metrics (
                id SERIAL PRIMARY KEY,
                signal VARCHAR(10),
                profitability DECIMAL(20,10),
                volatility DECIMAL(20,10),
                liquidity DECIMAL(20,10),
                price_momentum DECIMAL(20,10),
                relative_volume DECIMAL(20,10),
                spread DECIMAL(20,10),
                price_stability DECIMAL(20,10),
                historical_buy_comparison DECIMAL(20,10),
                historical_sell_comparison DECIMAL(20,10),
                medium_sell DECIMAL(10,1),
                medium_buy DECIMAL(10,1),
                possible_profit DECIMAL(20,10),
                current_price DECIMAL(20,10),
                instant_sell DECIMAL(20,10),
                search_query VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await conn.close()

class DatabaseManager:
    def __init__(self):
        self.pool = None
        self.lock = Lock()

    async def get_pool(self):
        if self.pool is None:
            self.pool = await self.create_pool()
        return self.pool

    async def create_pool(self):
        return await asyncpg.create_pool(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port,
            min_size=5,
            max_size=10
        )

    async def close_pool(self):
        if self.pool:
            await self.pool.close()

class DB_Operations:
    def __init__(self):
        self.db_manager = DatabaseManager()

    async def execute_query(self, query, *args):
        """Async SQL query using the db pool"""
        pool = await self.db_manager.get_pool()
        async with pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def upsert_signal_data(self, signal_data: SignalData):
        delete_query = "DELETE FROM data_metrics WHERE search_query = $1"

        insert_query = """
        INSERT INTO data_metrics
        (signal, profitability, volatility, liquidity, price_momentum, 
        relative_volume, spread, price_stability, historical_buy_comparison, 
        historical_sell_comparison, medium_sell, medium_buy, possible_profit, 
        current_price, instant_sell, search_query)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        """
        values = (
            signal_data.Signal,
            signal_data.metrics.profitability,
            signal_data.metrics.volatility,
            signal_data.metrics.liquidity,
            signal_data.metrics.price_momentum,
            signal_data.metrics.relative_volume,
            signal_data.metrics.spread,
            signal_data.metrics.price_stability,
            signal_data.metrics.historical_buy_comparison,
            signal_data.metrics.historical_sell_comparison,
            signal_data.metrics.medium_sell,
            signal_data.metrics.medium_buy,
            signal_data.metrics.possible_profit,
            signal_data.metrics.current_price,
            signal_data.metrics.instant_sell,
            signal_data.metrics.search_query
        )

        try:
            pool = await self.db_manager.get_pool()
            async with pool.acquire() as conn:
                await conn.execute(delete_query, signal_data.metrics.search_query)
                await conn.execute(insert_query, *values)
            logger.info("Data upserted successfully", extra={
                'search_query': signal_data.metrics.search_query,
                'signal': signal_data.Signal,
                'operation': 'upsert'
            })
        except Exception as e:
            logger.error("Database upsert error", extra={
                'error': str(e),
                'error_type': type(e).__name__,
                'search_query': signal_data.metrics.search_query,
                'operation': 'upsert'
            })

    async def read_signal_data(self, search_query: str):
        query = "SELECT * FROM data_metrics WHERE search_query = $1"
        try:
            async with self.db_manager.get_pool() as conn:
                result = await conn.fetch(query, search_query)
                logger.info("Data read successfully", extra={
                    'search_query': search_query,
                    'found_records': len(result) if result else 0,
                    'operation': 'read'
                })
                return result
        except Exception as e:
            logger.error("Database read error", extra={
                'error': str(e),
                'error_type': type(e).__name__,
                'search_query': search_query,
                'operation': 'read'
            })
            return None

    async def delete_signal_data(self, search_query: str):
        query = "DELETE FROM data_metrics WHERE search_query = $1"
        try:
            async with self.db_manager.get_pool() as conn:
                await conn.execute(query, search_query)
            logger.info("Data deleted successfully", extra={
                'search_query': search_query,
                'operation': 'delete'
            })
        except Exception as e:
            logger.error("Database delete error", extra={
                'error': str(e),
                'error_type': type(e).__name__,
                'search_query': search_query,
                'operation': 'delete'
            })

    async def get_random_five(self):
        """Get 5 sql objects, that are profitable"""
        query = """
        SELECT id, signal, profitability, volatility, liquidity 
        FROM data_metrics 
        WHERE signal = 'BUY' 
        ORDER BY RANDOM() 
        LIMIT 5
        """
        try:
            async with self.db_manager.get_pool() as conn:
                result = await conn.fetch(query)  # This might not work and might break get random five if it does just default back
                logger.info("Retrieved random records", extra={
                    'records_found': len(result) if result else 0,
                    'operation': 'random_select'
                })
                return result
                # return this instead if it bugs: return await conn.fetch(query)
        except Exception as e:
            logger.error("Database random select error", extra={
                'error': str(e),
                'error_type': type(e).__name__,
                'operation': 'random_select'
            })
            return None




