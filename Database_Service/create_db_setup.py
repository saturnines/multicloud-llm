import asyncio
import os
from dotenv import load_dotenv
import asyncpg
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DB_Setup')


load_dotenv('DataBase.env')

db_host = os.getenv('HOST')
db_name = os.getenv('DBNAME')
db_user = os.getenv('USER')
db_password = os.getenv('PASSWORD')
db_port = os.getenv('DB_PORT')


async def add_unique_constraint():
    try:
        conn = await asyncpg.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )

        constraint_exists = await conn.fetchval("""
            SELECT 1 FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE c.conname = 'data_metrics_search_query_signal_key'
            AND n.nspname = 'public'
        """)

        if not constraint_exists:
            logger.info("Adding unique constraint on search_query and signal...")
            await conn.execute("""
                ALTER TABLE data_metrics 
                ADD CONSTRAINT data_metrics_search_query_signal_key 
                UNIQUE (search_query, signal)
            """)
            logger.info("Unique constraint added successfully.")
        else:
            logger.info("Unique constraint already exists.")

        await conn.close()
        return True
    except Exception as e:
        logger.error(f"Error adding unique constraint: {e}")
        return False


async def create_database():
    """Create the database if it doesn't exist"""
    try:
        system_conn = await asyncpg.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database='postgres',
            port=db_port
        )

        # check if the db exists
        exists = await system_conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", db_name
        )

        if not exists:
            logger.info(f"Creating database '{db_name}'...")
            await system_conn.execute(f"CREATE DATABASE {db_name}")
            logger.info(f"Database '{db_name}' created successfully.")
        else:
            logger.info(f"Database '{db_name}' already exists.")

        await system_conn.close()
        return True
    except Exception as e:
        logger.error(f"Error checking/creating database: {e}")
        return False


async def create_table():
    """Create the data_metrics table"""
    try:
        conn = await asyncpg.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )


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

        logger.info("Table 'data_metrics' created or already exists.")
        await conn.close()
        return True
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False


async def main():
    """Run the complete database setup process"""
    logger.info("Starting database setup process...")


    db_created = await create_database()
    if not db_created:
        logger.error("Failed to create database. Exiting.")
        return False


    table_created = await create_table()
    if not table_created:
        logger.error("Failed to create table. Exiting.")
        return False

    constraint_added = await add_unique_constraint()
    if not constraint_added:
        logger.error("Failed to add unique constraint. Exiting.")
        return False

    logger.info("Database setup completed successfully!")
    return True


if __name__ == "__main__":
    success = asyncio.run(main())

    if success:
        print(" Database and table created")
    else:
        print("Database setup failed... ")