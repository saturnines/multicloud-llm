import collections
import threading
import time


class ApiCircuitBreakers:
    def __init__(self, api_count, soft_limit, hard_limit, rate_limit):
        """
        :param __seconds: Internal count to reset api_count
        :param api_count:  This is the current count of the API
        :param soft_limit:  Threshold for the api to go into half-open
        :param hard_limit   : Threshold for the api to go to Open
        :param rate_limit:  Hard Rate limit of the external
        :para queue: queue system to be used when half-open
        """
        # This part is the internal count
        self._lock = threading.Lock()
        self._last_reset_time = time.time()


        # This part is related to circut breakers
        self._current_status = "CLOSED"
        self._rate_limit = rate_limit
        self._api_count = api_count
        self._soft_limit = soft_limit
        self._hard_limit = hard_limit

        # Queue to handle requests
        self._queue = collections.deque()

    def get_rate_limit(self):
        return self._rate_limit

    def get_api_count(self):
        return self._api_count

    def get_soft_limit(self):
        return self._soft_limit

    def get_hard_limit(self):
        return self._hard_limit

    def __check_and_reset_count(self):
        current_time = time.time()
        if current_time - self._last_reset_time >= 60:
            self._api_count = 0
            self._last_reset_time = current_time


    def get_status(self):
        """May need to change this in the future"""
        with self._lock:
            self.__check_and_reset_count()
            if self._api_count <= self._soft_limit:
                self._current_status = "CLOSED"
            elif self._soft_limit < self._api_count <= self._hard_limit:
                self._current_status = "HALF"
            else:
                self._current_status = "OPEN"
            return self._current_status


    def insert_queue(self, func, args, kwargs):
        """Add arg to front of queue"""
        self._queue.append((func, args, kwargs))

    def process_queue(self):
        if self._queue:
            func, args, kwargs = self._queue.popleft()
            return func(*args, **kwargs)
        return None

    def handle_half_open(self,func, *args, **kwargs):
        if self._queue:
            return self.process_queue()
        else:
            self.insert_queue(func, args, kwargs)
            return None

    def handle_closed(self,func, *args, **kwargs):
        return func(*args, **kwargs)


    def handle_open(self, func, *args, **kwargs):
        self.insert_queue(func, args, kwargs)
        return None

    def __call__(self, func):
        def wrapped_func(*args, **kwargs):
            with self._lock:
                self._api_count += 1
                status = self.get_status()

            if status == "CLOSED":
                return self.handle_closed(func, *args, **kwargs)
            elif status == "HALF":
                return self.handle_half_open(func, *args, **kwargs)
            elif status == "OPEN":
                return self.handle_open(func, *args, **kwargs)

        return wrapped_func



# NEed to add logging too. # Will add try except blocks once I add logging.