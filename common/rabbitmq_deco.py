from functools import wraps
import time
import threading
import psutil
import os
from common.rabbitmq_publisher import publish_metrics


def rabbitmq_monitor(service_name, enable=True):
    """wrapper to publish metrics to rabbitmq, including CPU and memory usage"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not enable:
                return func(*args, **kwargs)

            # get pid for process tracking
            process = psutil.Process(os.getpid())

            # check starting
            start_time = time.time()
            start_cpu = process.cpu_percent()
            start_memory = process.memory_info().rss / (1024 * 1024)  # MB

            # execute function
            result = func(*args, **kwargs)

            # check ending
            end_time = time.time()
            end_memory = process.memory_info().rss / (1024 * 1024)  # MB
            end_cpu = process.cpu_percent()

            # calculate how long, memory usage, cpu usage
            execution_time = end_time - start_time
            memory_delta = end_memory - start_memory
            cpu_usage = end_cpu - start_cpu

            # metric data
            metrics_data = {
                'function_name': func.__name__,
                'execution_time': execution_time,
                'memory_start_mb': start_memory,
                'memory_end_mb': end_memory,
                'memory_delta_mb': memory_delta,
                'cpu_usage': cpu_usage,
                'thread_count': threading.active_count(),
                'timestamp': time.time()
            }

            # send to rabbitmq
            publish_metrics(service_name, 'function_metrics', metrics_data)

            return result

        return wrapper

    return decorator