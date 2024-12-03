from dotenv import load_dotenv
load_dotenv('DataBase.env')
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaConsumer
import threading
import json
import uvicorn
import os
from typing import Optional
from SQLDataBase import *

app = FastAPI()


kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

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
            return {"success"}
        except Exception as e:
            print(f"Failed to send data to DB: {e}")
            raise HTTPException(status_code=500, detail="DB Error!")

    async def read_data(self, data_model):
        try:
            read_data = await self.DataBaseCRUD.read_signal_data(data_model)
            return read_data
        except Exception as e:
            print(f"Failed to get data from DB: {e}")
            raise HTTPException(status_code=500, detail="DB Error!")

    async def delete_data(self, data_model):
        try:
            await self.DataBaseCRUD.delete_signal_data(data_model)
            return {"message": "Data Deleted!!"}
        except Exception as e:
            print(f"Failed to get data from DB: {e}")
            raise HTTPException(status_code=500, detail="DB Error!")

    async def get_random_data(self):
        try:
            data = await self.DataBaseCRUD.get_random_five()
            return data
        except Exception as e:
            print(f"Failed to get data from DB: {e}")
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
        self.consumer_thread = threading.Thread(target=self._consume_messages, daemon=True)
        self.consumer_thread.start()

    async def _consume_messages(self):
        try:
            print("Starting database consumer...")
            for message in self.consumer:
                data = message.value
                if data:
                    signal_data = SignalDataModel(
                        Signal=data['signal'],
                        metrics=SignalData(**data['metrics'])
                    )
                    await self.event_bus.upsert_create(signal_data)
                    print(f"Stored data for: {data['metrics'].get('search_query')}")
        except Exception as e:
            print(f"Consumer error: {e}")
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
        print(f"Error as {e}, check logs.")
        raise HTTPException(status_code=500, detail="DB Error!")

@app.post("/api/v1/delete_data/")
async def delete_data(data: str):
    try:
        return await EventBus.delete_data(data)
    except Exception as e:
        print(f"Error as {e}, check logs.")
        raise HTTPException(status_code=500, detail="DB Error!")

@app.get("/api/v1/random_data/")
async def get_random_data():
    try:
        data = await EventBus.get_random_data()
        return data
    except Exception as e:
        print(f"Error as {e}, check logs.")
        raise HTTPException(status_code=500, detail="DB Error!")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)