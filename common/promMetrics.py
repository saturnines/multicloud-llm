from functools import wraps
import time
import psutil
import os

from prometheus_client import Counter, Histogram, Gauge, start_http_server

# metrics
REQUEST_COUNT = Counter('request_count', 'Total number of requests', ['service', 'function'])
REQUEST_LATENCY = Histogram('request_latency_seconds', 'Request latency in seconds', ['service', 'function'])
MEMORY_USAGE = Gauge('memory_usage_mb', 'Memory usage in MB', ['service', 'function'])
CPU_USAGE = Gauge('cpu_usage_percent', 'CPU usage percentage', ['service'])
# I might need to add threading ??


_prometheus_servers = {}


def start_prometheus_server(service_name, port=8000):
    """Start a Prometheus metrics HTTP server for a specific service"""
    if service_name not in _prometheus_servers:
        try:
            start_http_server(port)
            _prometheus_servers[service_name] = port
            print(f"Started Prometheus metrics server for {service_name} on port {port}")
        except Exception as e:
            print(f"Error starting Prometheus server: {e}")


def prometheus_monitor(service_name):
    """Decorator to monitor function execution and expose metrics to Prometheus"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()

            # Track metrics as defined above
            process = psutil.Process(os.getpid())
            start_memory = process.memory_info().rss / (1024 * 1024)  # MB
            start_cpu = process.cpu_percent()
            REQUEST_COUNT.labels(service=service_name, function=func.__name__).inc()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            REQUEST_LATENCY.labels(service=service_name, function=func.__name__).observe(execution_time)
            end_memory = process.memory_info().rss / (1024 * 1024)  # MB
            MEMORY_USAGE.labels(service=service_name, function=func.__name__).set(end_memory - start_memory)
            CPU_USAGE.labels(service=service_name).set(process.cpu_percent() - start_cpu)

            return result

        return wrapper

    return decorator
