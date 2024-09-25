import heapq
from typing import Optional, List, Dict
import json


class HeapNode:
    """Hold various financial metrics."""

    def __init__(self,
                 signal: [str] = None,
                 profitability: Optional[float] = 0,
                 volatility: Optional[float] = 0,
                 liquidity: Optional[float] = 0,
                 price_stability: Optional[float] = 0,
                 relative_volume: Optional[float] = 0,
                 possible_profit: Optional[float] = 0,
                 current_price: Optional[float] = 0,
                 search_query: Optional[str] = None):

        # Initialize attributes
        self.signal = signal
        self.profitability = profitability
        self.volatility = volatility
        self.liquidity = liquidity
        self.price_stability = price_stability
        self.relative_volume = relative_volume
        self.possible_profit = possible_profit
        self.current_price = current_price
        self.search_query = search_query
        self.rank = None
        self.adjust_rank()

    def get_search_query(self) -> Optional[str]:
        return self.search_query

    def get_signal(self) -> [str]:
        return self.signal

    def get_profitability(self) -> Optional[float]:
        return self.profitability

    def get_volatility(self) -> Optional[float]:
        return self.volatility

    def get_liquidity(self) -> Optional[float]:
        return self.liquidity

    def get_price_stability(self) -> Optional[float]:
        return self.price_stability

    def get_relative_volume(self) -> Optional[float]:
        return self.relative_volume

    def get_possible_profit(self) -> Optional[float]:
        return self.possible_profit

    def get_current_price(self) -> Optional[float]:
        return self.current_price

    def get_rank(self) -> Optional[int]:
        return self.rank

    def adjust_rank(self) -> Optional[int]:
        self.rank = self._calculate()  # Set the calculated rank
        return self.rank  # Optionally return the rank

    def _calculate(self) -> int:
        """Trading algo for Cache - Calculates a score based on metrics."""
        res = 0

        # Check signal and assign points
        if self.get_signal() == "BUY":
            res += 100
        elif self.get_signal() == "WATCH":
            res += 50
        else:
            res += 0

        # Adjust for profit
        if self.get_profitability() > 0:
            res += 20

        # Adjust for volatility
        if self.get_volatility() < -5:
            res -= 10
        elif self.get_volatility() > 0:
            res += 10

        # Adjust for liquidity
        if self.get_liquidity() > 500000:
            res += 30

        # Adjust for price stability
        if self.get_price_stability() > 50:
            res += 15

        # Adjust for relative volume
        if self.get_relative_volume() > 0.1:
            res += 10

        # Adjust for possible profit
        if self.get_possible_profit() > 0:
            res += 25

        return res

class TopNCache:
    def __init__(self, k: int):
        self.HeapCache = []
        self.cap = k

    def add(self, heap_node: HeapNode):
        heap_node.adjust_rank()
        if len(self.HeapCache) < self.cap:
            heapq.heappush(self.HeapCache, (-heap_node.get_rank(), heap_node))
        elif -heap_node.get_rank() > self.HeapCache[0][0]:
            heapq.heapreplace(self.HeapCache, (-heap_node.get_rank(), heap_node))

    def get_cache(self):
        """Return the cache as a JSON string"""
        return json.dumps([
            self.extract_node_data(node)
            for _, node in sorted(self.HeapCache, reverse=True)
        ])

    @staticmethod
    def extract_node_data(heap_node: HeapNode):
        return {
            "name": heap_node.get_search_query(),
            "signal": heap_node.get_signal(),
            "profitability": heap_node.get_profitability(),
            "volatility": heap_node.get_volatility(),
            "liquidity": heap_node.get_liquidity(),
            "price_stability": heap_node.get_price_stability(),
            "relative_volume": heap_node.get_relative_volume(),
            "possible_profit": heap_node.get_possible_profit(),
            "current_price": heap_node.get_current_price(),
            "rank": heap_node.get_rank()
        }

    def get_averages(self) -> Dict[str, float]:
        """Calculate and return average metrics"""
        if not self.HeapCache:
            return {}

        metrics = [
            "profitability", "volatility", "liquidity", "price_stability",
            "relative_volume", "possible_profit", "current_price", "rank"
        ]

        sums = {metric: 0 for metric in metrics}
        count = len(self.HeapCache)

        for _, node in self.HeapCache:
            for metric in metrics:
                value = getattr(node, f"get_{metric}")()
                if value is not None:
                    sums[metric] += value

        averages = {metric: sums[metric] / count for metric in metrics if sums[metric] != 0}

        return averages

    def get_cache_with_averages(self):
        """Return the cache data along with averages as a JSON string"""
        cache_data = json.loads(self.get_cache())
        averages = self.get_averages()

        result = {
            "cache_data": cache_data,
            "averages": averages
        }

        return json.dumps(result)

# TODO TEST




