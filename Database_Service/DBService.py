from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv('DataBase.env')
import os
from fastapi import FastAPI, HTTPException

app = FastAPI()
import SQLDataBase


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

class DatabaseManager:
    def __init__(self):
        self.db = SQLDataBase.DatabaseManager()
        self.dbCRUD = SQLDataBase.DB_Operations()
        self.app = FastAPI()
        self.create = SQLDataBase.DataBaseCreator()

    @app.post("/api/v1/create_data/")
    async def create_data(self, data_model):
        try:
            await self.dbCRUD.upsert_signal_data(data_model)
            return {"message": "Data sent to the DB successfully!"}
        except Exception as e:
            print(f"Failed to send data to DB: {e}")
            raise HTTPException(status_code=500, detail="DB Error!")

    @app.post("/api/v1/delete_data/")
    async def delete_from_db(self, query):
        try:
            await self.dbCRUD.delete_signal_data(query)
            return {"message": "Data removed from the DB successfully!"}

        except Exception as e:
            print(f"Failed to delete data from DB: {e}")
            raise HTTPException(status_code=500, detail="DB Error!")

    @app.post("/api/v1/read_data/")
    async def read_from_db(self, query):
        try:
            return await self.dbCRUD.read_signal_data(query)
            # Add logging here
        except Exception as e:
            print(f"Failed to read data from DB: {e}")
            raise HTTPException(status_code=500, detail="DB Error!")

    @app.post("/api/v1/random_data/")
    async def get_random_five(self):
        try:
            return await self.dbCRUD.get_random_five()
        except Exception as e:
            print(f"Failed to random data from DB: {e}")
            raise HTTPException(status_code=500, detail="DB Error!")

if __name__ == "__main__":
    import uvicorn
    x = DatabaseManager()
    uvicorn.run(app, host="0.0.0.0", port=8001)