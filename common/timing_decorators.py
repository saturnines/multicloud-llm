from functools import wraps
import time
import psutil
import os
import logging
from fluent import sender
import threading


def configure_logging(service_name: str, fluentd_host: str = 'localhost', fluentd_port: int = 24224):
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fluent_sender = sender.FluentSender(service_name, host=fluentd_host, port=fluentd_port)

        class FluentHandler(logging.Handler):
            def emit(self, record):

                log_data = {
                    'level': record.levelname,
                    'message': self.format(record),
                    'service': service_name
                }

                # measuring different metrics
                if hasattr(record, 'extra') and isinstance(record.extra, dict):
                    if record.extra.get('metric_type'):
                        log_data.update({
                            'metrics': {
                                'type': record.extra.get('metric_type'),
                                'execution_time': record.extra.get('execution_time'),
                                'memory_delta_mb': record.extra.get('memory_delta_mb'),
                                'cpu_usage': record.extra.get('cpu_usage'),
                                'thread_count': record.extra.get('thread_count'),
                            }
                        })
                    log_data.update(record.extra)

                fluent_sender.emit(record.name, log_data)

        fluent_handler_instance = FluentHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fluent_handler_instance.setFormatter(formatter)
        logger.addHandler(fluent_handler_instance)

    return logger


logger = configure_logging('Performance_Metrics')


def get_memory():
    process = psutil.Process(os.getpid())
    memory_information = process.memory_info()
    return memory_information.rss / (1024 * 1024)  # MB


def get_cpu_usage():
    return psutil.cpu_percent(interval=None)


def toggle_time(enable=False):
    def log_time(func):
        @wraps(func)
        def time_wrapper(*args, **kwargs):
            if enable:
                start = time.time()
                result = func(*args, **kwargs)
                time_took = time.time() - start
                logger.info("Function execution completed", extra={
                    'execution_time': time_took,
                    'function_name': func.__name__,
                    'metric_type': 'timing'
                })
                return result
            return func(*args, **kwargs)

        return time_wrapper

    return log_time


def toggle_memory(enable=False):
    def log_memory(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if enable:
                start_memory = get_memory()
                result = func(*args, **kwargs)
                end_memory = get_memory()
                logger.info("Memory usage tracked", extra={
                    'start_memory_mb': start_memory,
                    'end_memory_mb': end_memory,
                    'memory_delta_mb': end_memory - start_memory,
                    'function_name': func.__name__,
                    'metric_type': 'memory'
                })
                return result
            return func(*args, **kwargs)

        return wrapper

    return log_memory


def monitor_all(enable=False):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not enable:
                return func(*args, **kwargs)

            start_time = time.time()
            start_memory = get_memory()
            start_cpu = get_cpu_usage()

            result = func(*args, **kwargs)

            logger.info("Resource metrics", extra={
                'function_name': func.__name__,
                'execution_time': time.time() - start_time,
                'memory_delta_mb': get_memory() - start_memory,
                'cpu_usage': get_cpu_usage() - start_cpu,
                'thread_count': threading.active_count(),
                'metric_type': 'all'
            })

            return result

        return wrapper

    return decorator