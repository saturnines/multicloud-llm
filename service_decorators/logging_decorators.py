import os
import time
from functools import wraps

import psutil


# Measure Time.
def toggle_time(enable=False):
    def log_time(func):
        """This logs the time."""
        @wraps(func)
        def time_wrapper(*args, **kwargs):
            if enable:
                start = time.time()
                result = func(*args, **kwargs)
                time_took = time.time() - start
                print(f"Took {time_took:.6f} seconds")
                return result
            else:
                # Just return as normal
                return func(*args, **kwargs)
        return time_wrapper
    return log_time


# Helper Function for memory logging
def get_memory():
    process = psutil.Process(os.getpid())
    memory_information = process.memory_info()
    return memory_information.rss / (1024 * 1024) # This is MB.


# Measure memory.
def toggle_memory(enable=False):
    def log_memory(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if enable:
                start_memory = get_memory()
                result = func(*args, **kwargs)
                end_memory = get_memory()
                print(f"Starting memory {start_memory}, Ending memory {end_memory}, total {end_memory - start_memory}")
                return result
            else:
                return func(*args, **kwargs)
        return wrapper
    return log_memory

# Need to add a hook a logger either efk or grafana/prom to these once the logging service is done.
