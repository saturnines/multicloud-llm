import threading

class ApiCircutBreakers:
    def __init__(self, api_count, soft_limit, hard_limit, rate_limit):
        """
        :param seconds: Internal count to reset api_count
        :param api_count:  This is the current count of the API
        :param soft_limit:  Threshold for the api to go into half-open
        :param w    : Threshold for the api to go to Open
        :param rate_limit:  Hard Rate limit of the external
        :para queue: queue system to be used when half-open
        """
        # This part is the internal count
        self.__seconds = 0
        self._run()


        # This part is related to circut breakers
        self._current_status = "CLOSED"
        self._rate_limit = rate_limit
        self._api_count = api_count
        self._soft_limit = soft_limit
        self._hard_limit = hard_limit
        self.queue = []


    def get_rate_limit(self):
        return self._rate_limit

    def get_api_count(self):
        return self._api_count

    def get_soft_limit(self):
        return self._soft_limit

    def get_hard_limit(self):
        return self._hard_limit

    def _run(self):
        self.__seconds += 1

        if self.__seconds % 60 == 0:
            self._api_count()

        threading.Timer(1, self._run).start()

    def reset_count(self):
        self._api_count = 0

    def get_status(self):
        current_count = self.get_api_count()
        soft_limit = self.get_soft_limit()
        hard_limit = self.get_hard_limit

        if current_count <= soft_limit:
            """Set this to CLOSED"""
            self._current_status = "CLOSED"

        if current_count >= soft_limit and current_count < hard_limit:
            self._current_status = "HALF"
            """Set this to Half-Open"""

        if current_count > hard_limit:
            self._current_status = "CLOSED"
            """Set this to closed."""
        return self._current_status

    def handle_closed(self):
        pass

    def handle_half_open(self):
        pass


    def handle_open(self):
        pass

    def __call__(self, *args,**kwargs):
        pass


# TODO Finish deco for circut wrappers
