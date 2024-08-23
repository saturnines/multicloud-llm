class ApiCircutBreakers:
    def __init__(self, api_count, soft_limit, hard_limit, rate_limit ):
        """

        :param api_count:  This is the current count of the API
        :param soft_limit:  Threshold for the api to go into half-open
        :param hard_limit: Threshold for the api to go to Open
        :param rate_limit:  Hard Rate limit of the external
        :para queue: queue system to be used when half-open
        """

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
        return self._soft_limit:


    def __call__

# TODO Estiamte rate limits
# TODO Finish deco for circut wrappers