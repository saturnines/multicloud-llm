import asyncio
from dotenv import load_dotenv
load_dotenv('DataBase.env')
from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer
import threading
import json
import uvicorn
from SQLDataBase import *

app = FastAPI()

kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
from DatabaseLogConfig import configure_logging
logger = configure_logging('DB_Gateway')

class SignalData(BaseModel):
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

class SignalDataModel(BaseModel):
    Signal: str
    metrics: SignalData

class DataBaseInfo:
    def __init__(self):
        self.db_host = os.getenv('HOST')
        self.db_name = os.getenv('DBNAME')
        self.db_user = os.getenv("USER")
        self.db_password = os.getenv("PASSWORD")
        self.db_port = os.getenv("DB_PORT")

    def get_host(self):
        return self.db_host

    def get_db_name(self):
        return self.db_name

    def get_db_user(self):
        return self.db_user

    def get_db_password(self):
        return self.db_password

    def get_db_port(self):
        return self.db_port

class DatabaseBusHelper:
    def __init__(self):
        self.DataBaseInfo = DataBaseInfo()
        self.DataBaseManager = DatabaseManager()
        self.DataBaseCRUD = DB_Operations()
        self.DataBaseCreate = DataBaseCreator()

    async def upsert_create(self, data_model):
        try:
            await self.DataBaseCRUD.upsert_signal_data(data_model)
            logger.info("Successfully upserted data", extra={
                'signal': data_model.Signal,
                'search_query': data_model.metrics.search_query
            })
            return {"success"}
        except Exception as e:
            logger.error("Failed to upsert data to DB", extra={
                'error': str(e),
                'error_type': type(e).__name__,
                'signal': data_model.Signal
            })
            raise HTTPException(status_code=500, detail="DB Error!")

    async def read_data(self, data_model):
        try:
            read_data = await self.DataBaseCRUD.read_signal_data(data_model)
            logger.info("Successfully read data", extra={
                'search_query': data_model
            })
            return read_data
        except Exception as e:
            logger.error(f"Failed to get read data from DB: {e}")
            raise HTTPException(status_code=500, detail="DB Error!")

    async def delete_data(self, data_model):
        try:
            await self.DataBaseCRUD.delete_signal_data(data_model)
            logger.info("Successfully deleted data", extra={
                'search_query': data_model
            })
            return {"message": "Data Deleted!!"}
        except Exception as e:
            logger.error(f"Failed to delete data from DB: {e}")
            raise HTTPException(status_code=500, detail="DB Error!")

    async def get_random_data(self):
        try:
            data = await self.DataBaseCRUD.get_random_five()
            logger.info("Successfully retrieved random data", extra={ # Not sure this will work, if something bug checks if this bugged.
                'data_count': len(data) if data else 0
            })
            return data
        except Exception as e:
            print(f"Failed to get random data from DB: {e}")
            raise HTTPException(status_code=500, detail="DB Error!")

class DatabaseConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'database_operations',
            bootstrap_servers=kafka_bootstrap_servers,
            group_id='db_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.event_bus = DatabaseBusHelper()
        self.loop = asyncio.new_event_loop()
        self.consumer_thread = threading.Thread(target=self._consume_messages, daemon=True)
        self.consumer_thread.start()

    def _consume_messages(self):
        asyncio.set_event_loop(self.loop)
        try:
            logger.info("Starting database consumer...")
            for message in self.consumer:
                try:
                    data = message.value
                    logger.debug(f"Received database message: {data}")  # Debug logging

                    if data and isinstance(data, dict):
                        signal_data = SignalDataModel(
                            Signal=data.get('signal') or data.get('Signal'),  # Handle both cases
                            metrics=SignalData(**data['metrics'])
                        )
                        self.loop.run_until_complete(
                            self.event_bus.upsert_create(signal_data)
                        )
                        logger.info(
                            f"Stored data for: {data['metrics'].get('search_Query') or data['metrics'].get('search_query')}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.consumer.close()

EventBus = DatabaseBusHelper()
db_consumer = DatabaseConsumer()

@app.get("/api/v1/read_data/")
async def read_data(data: str):
    try:
        data = await EventBus.read_data(data)
        return data
    except Exception as e:
        logger.error(f"Error as {e}, failed to read data.")
        raise HTTPException(status_code=500, detail="DB Error!")

@app.post("/api/v1/delete_data/")
async def delete_data(data: str):
    try:
        return await EventBus.delete_data(data)
    except Exception as e:
        logger.error(f"Error as {e}, failed to delete data.")
        raise HTTPException(status_code=500, detail="DB Error!")

@app.get("/api/v1/random_data/")
async def get_random_data():
    try:
        data = await EventBus.get_random_data()
        return data
    except Exception as e:
        logger.error(f"Error as {e}, failed to get random data from API endpoint")
        raise HTTPException(status_code=500, detail="DB Error!")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)