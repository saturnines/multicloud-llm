import unittest
import asyncio
import sys
import aiohttp
from Data_Ingestion_Service.service_breakers_deco import ApiCircuitBreakers

async def process_all_messages(api_breaker):
    # Testing function to process all messages from kafka
    while True:
        result = await api_breaker.process_from_queue()
        if result:
            print(f"Processed task from queue: {result}")
        else:
            print("Queue is empty, stopping the process.")
            break

async def query_api(search_term):
    """Async function to query the external API with a given search term."""
    url = f"https://api.kevinsapi.net/items/?search_term={search_term}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                print(f"Results for {search_term}: {data}")
                return data
            else:
                print(f"Failed to query {search_term}. Status Code: {response.status}")
                return None

async def main():
    try:
        # enqueue tasks
        api_breaker = ApiCircuitBreakers(api_count=0, soft_limit=10, hard_limit=20, rate_limit=5)
        api_breaker.enqueue_tasks()

        await asyncio.sleep(5)
        await process_all_messages(api_breaker)

        # Popping queries from the queue
        x = api_breaker._queue.popleft()
        y = api_breaker._queue.popleft()
        z = api_breaker._queue.popleft()

        print(f"Processing queries: {x}, {y}, {z}")

        # Query API
        await query_api(x)
        await query_api(y)
        await query_api(z)

        return True
    except Exception as e:
        print(f"main() raised an exception: {e}")
        return False

class TestMainFunction(unittest.IsolatedAsyncioTestCase):
    async def test_main(self):
        try:
            await main()
        except Exception as e:
            self.fail(f"main() raised an exception: {e}")

if __name__ == "__main__":
    # return result
    test_suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestMainFunction)
    test_runner = unittest.TextTestRunner()
    test_result = test_runner.run(test_suite)

    if test_result.wasSuccessful():
        print('True')
        sys.exit(0)
    else:
        print('False')
        sys.exit(1)