
from dotenv import load_dotenv


load_dotenv('DataBase.env')


import os
from fastapi import FastAPI
import SQLDataBase  # Assuming this is your module with DB operations

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

    async def send_to_db(self, query):
        try:
            await self.dbCRUD.upsert_signal_data(query)
        except Exception as e:
            print(f"Failed to send data to DB: {e}")
            # You can re-raise the exception if needed
            # raise

    async def delete_from_db(self, query):
        try:
            await self.dbCRUD.delete_signal_data(query)
        except Exception as e:
            print(f"Failed to delete data from DB: {e}")
            # raise

    async def read_from_db(self, query):
        try:
            return await self.dbCRUD.read_signal_data(query)
        except Exception as e:
            print(f"Failed to read data from DB: {e}")
            return None

    async def get_random_five(self):
        try:
            return await self.dbCRUD.get_random_five()
        except Exception as e:
            print(f"Failed to retrieve random five entries from DB: {e}")
            return []

# TODO
# make routes
# should be finished , remember  start the db
# add logging . health checks

