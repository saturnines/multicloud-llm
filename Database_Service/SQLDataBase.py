from decimal import Decimal
from typing import List, Optional, Dict, Any,
from pydantic import BaseModel
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor
from contextlib import contextmanager
import os

load_dotenv('DataBase.env')

# Variables
db_host = os.getenv('HOST')
db_name = os.getenv('DBNAME')
db_user = os.getenv("USER")
db_password = os.getenv("PASSWORD")
db_port = os.getenv("DB_PORT")


class Metrics(BaseModel):
    profitability: Decimal
    volatility: Decimal
    liquidity: Decimal
    price_momentum: Decimal
    relative_volume: Decimal
    spread: Decimal
    price_stability: Decimal
    historical_buy_comparison: Decimal
    historical_sell_comparison: Decimal
    medium_sell: Decimal
    medium_buy: Decimal
    possible_profit: Decimal
    current_price: Decimal
    instant_sell: Decimal
    search_query: Optional[str] = None


class SignalData(BaseModel):
    Signal: str
    metrics: Metrics


class DataBaseCreator:
    """Create DataBase"""

    def __init__(self):
        self.db_host = db_host
        self.db_user = db_user
        self.db_name = db_name
        self.db_password = db_password
        self.db_port = db_port

    def create_db(self):
        """Creates the db with schema"""
        conn = psycopg2.connect(host=self.db_host, dbname=self.db_host, user=self.db_user, password=self.db_password,
                                port=self.db_port)

        cur = conn.cursor()

        cur.execute("""
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
                search_query VARCHAR(255),  -- Add this line
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        cur.close()
        conn.close()


class DatabaseManager:
    def __init__(self):
        self.db_host = db_host
        self.db_user = db_user
        self.db_name = db_name
        self.db_password = db_password
        self.db_port = db_port

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.db_host,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                port=self.db_port
            )
            yield conn
        finally:
            if conn:
                conn.close()

    @contextmanager
    def get_cursor(self, commit=False):
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=DictCursor)
            try:
                yield cursor
                if commit:
                    conn.commit()
            finally:
                cursor.close()


class DB_Operations:
    def __init__(self):
        self.db_manager = DatabaseManager()

    def upsert_signal_data(self, signal_data: SignalData):
        # delete original data
        delete_query = "DELETE FROM data_metrics WHERE search_query = %s"

        # insert new data
        insert_query = """
        INSERT INTO data_metrics
        (signal, profitability, volatility, liquidity, price_momentum, 
        relative_volume, spread, price_stability, historical_buy_comparison, 
        historical_sell_comparison, medium_sell, medium_buy, possible_profit, 
        current_price, instant_sell, search_query)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            with self.db_manager.get_cursor(commit=True) as cur:
                cur.execute(delete_query, (signal_data.metrics.search_query,))

                cur.execute(insert_query, values)
            print("Data upserted successfully")
        except Exception as e:
            print(f"An error occurred during upsert: {e}")

    def read_signal_data(self, search_query: str):
        query = "SELECT * FROM data_metrics WHERE search_query = %s"
        try:
            with self.db_manager.get_cursor() as cur:
                cur.execute(query, (search_query,))
                return cur.fetchall()
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def delete_signal_data(self, search_query: str):
        query = "DELETE FROM data_metrics WHERE search_query = %s"
        try:
            with self.db_manager.get_cursor(commit=True) as cur:
                cur.execute(query, (search_query,))
            print("Data deleted successfully")
        except Exception as e:
            print(f"An error occurred: {e}")


