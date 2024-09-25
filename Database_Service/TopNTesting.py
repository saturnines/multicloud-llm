from Database_Service import TopNCache
from Database_Service.TopNCache import HeapNode
import pytest

x = TopNCache.TopNCache(5)
print(x.get_averages())

dummy_data = [
    {"signal": "BUY", "profitability": -44.40, "volatility": -2373.64, "liquidity": 62241.72, "price_stability": 109.22,
     "relative_volume": 1490952.23, "possible_profit": -0.038, "current_price": 84.18},
    {"signal": "WATCH", "profitability": -21.99, "volatility": -6223.85, "liquidity": 180598.04,
     "price_stability": 102.59, "relative_volume": 1244936.27, "possible_profit": 0.019, "current_price": 147.52},
    {"signal": "BUY", "profitability": -26.42, "volatility": -10716.42, "liquidity": 70127.19,
     "price_stability": 116.26, "relative_volume": 1717600.82, "possible_profit": -0.034, "current_price": 19.17},
    {"signal": "WATCH", "profitability": -27.71, "volatility": -2894.33, "liquidity": 15649.87,
     "price_stability": 101.99, "relative_volume": 1136641.61, "possible_profit": 0.120, "current_price": 31.46},
    {"signal": "BUY", "profitability": -76.81, "volatility": -2968.33, "liquidity": 26128.77, "price_stability": 106.66,
     "relative_volume": 1764154.09, "possible_profit": 0.236, "current_price": 164.55},
    {"signal": "SELL", "profitability": -26.41, "volatility": -11945.66, "liquidity": 27688.14,
     "price_stability": 117.98, "relative_volume": 1701859.33, "possible_profit": -0.011, "current_price": 123.40},
    {"signal": "BUY", "profitability": -39.63, "volatility": -3672.93, "liquidity": 139811.37,
     "price_stability": 105.57, "relative_volume": 1173544.90, "possible_profit": 0.039, "current_price": 35.55},
    {"signal": "BUY", "profitability": -58.10, "volatility": -8533.95, "liquidity": 151291.78,
     "price_stability": 118.99, "relative_volume": 1328089.90, "possible_profit": 0.078, "current_price": 45.41},
    {"signal": "BUY", "profitability": -97.48, "volatility": -17880.08, "liquidity": 118198.24,
     "price_stability": 117.83, "relative_volume": 1463852.02, "possible_profit": -0.005, "current_price": 117.03},
    {"signal": "WATCH", "profitability": -80.39, "volatility": -5006.48, "liquidity": 6496.71,
     "price_stability": 101.32, "relative_volume": 1446118.86, "possible_profit": -0.007, "current_price": 133.69}
]

god_node = HeapNode(
    signal="BUY",
    profitability=11.40,
    volatility=-2373.64,
    liquidity=151292.72,
    price_stability=899.22,
    relative_volume=1490952.23,
    possible_profit=0.1118,
    current_price=84.18
)


def create_heap_nodes():
    return [
        HeapNode(
            signal=data["signal"],
            profitability=data["profitability"],
            volatility=data["volatility"],
            liquidity=data["liquidity"],
            price_stability=data["price_stability"],
            relative_volume=data["relative_volume"],
            possible_profit=data["possible_profit"],
            current_price=data["current_price"]
        )
        for data in dummy_data
    ]


def test_init_topncache_with_nodes():
    cache = TopNCache.TopNCache(5)

    # heap nodes
    heap_nodes = create_heap_nodes()

    # Insert heap nodes into the cache
    for node in heap_nodes:
        cache.add(node)

    # Test that the cache holds only the top N nodes (top 5 in this case)
    assert len(cache.HeapCache) == 5, "Cache should only contain 5 nodes"

    # Test averages
    averages = cache.get_averages()
    assert averages is not None, "Averages should not be None"
    assert isinstance(averages, dict), "Averages should be a dictionary"

    # check some values of averages
    assert "profitability" in averages, "Averages should contain profitability"
    assert "liquidity" in averages, "Averages should contain liquidity"


def test_cache_functionality():
    cache = TopNCache.TopNCache(10)

    # heap nodes
    heap_nodes = create_heap_nodes()

    for node in heap_nodes:
        cache.add(node)

    assert len(cache.HeapCache) == 10, "Cache should only contain 10 nodes"

    # Test if adding new node works and top-n cache works by adding a "god" node

    cache.add(god_node)
    assert len(cache.HeapCache) == 10, "Cache should only contain 10 nodes"
    print(cache.get_cache())

    # Clear Cache
    cache.clear_cache()
    assert len(cache.HeapCache) == 0, "Cache is empty!"


if __name__ == "__main__":
    pytest.main()
