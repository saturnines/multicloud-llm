import logging
from fluent import sender
from typing import Dict, Any

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