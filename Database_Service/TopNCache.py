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
        """Helper function to calculate ranks"""

        # Initialize score
        score = 0

        # Weights
        profitability_weight = 2.0  # Higher weight for profits
        volatility_weight = -1.5  # Negative weight for high volatility
        liquidity_weight = 1.2  # Medium weight for liquidity (Not sure if I should do this higher or not)
        price_stability_weight = 0.8  # Lower weight for price stability
        relative_volume_weight = 0.4  # Lower weight for relative volume
        possible_profit_weight = 1.5  # High weight for possible profit

        # Signal weighting
        if self.get_signal() == "BUY":
            score += 100
        elif self.get_signal() == "WATCH":
            score += 50

        # Profitability: the higher, the better
        if self.get_profitability() is not None:
            score += self.get_profitability() * profitability_weight

        # Volatility: high vol is bad
        if self.get_volatility() is not None:
            if self.get_volatility() > 0:
                score -= self.get_volatility() * volatility_weight
            elif self.get_volatility() < -5:  # Penalize high vol (Should fine-tune this)
                score += self.get_volatility() * volatility_weight

        # liquidity check
        if self.get_liquidity() is not None and self.get_liquidity() > 500000:
            score += (self.get_liquidity() / 500000) * liquidity_weight  # Scale liquidity

        # price stability check more stability is good
        if self.get_price_stability() is not None:
            score += self.get_price_stability() * price_stability_weight

        # relative volume check
        if self.get_relative_volume() is not None:
            score += self.get_relative_volume() * relative_volume_weight

        # Possible Profit: biggest weight
        if self.get_possible_profit() is not None:
            score += self.get_possible_profit() * possible_profit_weight

        return round(score)


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
# Data might be sorted by least to most need to fix
