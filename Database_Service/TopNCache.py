from typing import Optional


class ListNode:
    """A class to hold various financial metrics."""

    def __init__(self,
                 signal: [str] = None,
                 profitability: Optional[float] = 0,
                 volatility: Optional[float] = 0,
                 liquidity: Optional[float] = 0,
                 price_stability: Optional[float] = 0,
                 relative_volume: Optional[float] = 0,
                 possible_profit: Optional[float] = 0,
                 current_price: Optional[float] = 0):
        # Initialize attributes
        self.signal = signal
        self.profitability = profitability
        self.volatility = volatility
        self.liquidity = liquidity
        self.price_stability = price_stability
        self.relative_volume = relative_volume
        self.possible_profit = possible_profit
        self.current_price = current_price
        self.rank = None

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
        rank = self._calculate()
        self.rank = rank
        return

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
        # adjust for profit

        if self.get_profitability() > 0:
            res += 20
        # adjust for get_vol
        if self.get_volatility() < -5:
            res -= 10
        elif self.get_volatility() > 0:
            res += 10
        # adjust for liquid
        if self.get_liquidity() > 500000:
            res += 30
        # adjust for price stab
        if self.get_price_stability() > 50:
            res += 15
        # just for rel vol
        if self.get_relative_volume() > 0.1:
            res += 10

        # Adjust for profit
        if self.get_possible_profit() > 0:
            res += 25

        return res


class TopNCache:
    def __init__(self, capacity: int):
        self.cache = {}
        self.cap = capacity






