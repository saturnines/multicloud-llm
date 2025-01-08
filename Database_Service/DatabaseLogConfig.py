import logging
from fluent import sender


def configure_logging(service_name: str, fluentd_host: str = 'localhost', fluentd_port: int = 24224):
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fluent_sender = sender.FluentSender(service_name, host=fluentd_host, port=fluentd_port)

        class FluentHandler(logging.Handler):
            def emit(self, record):
                fluent_sender.emit(record.name, {
                    'level': record.levelname,
                    'message': self.format(record),
                    'service': service_name
                })

        fluent_handler_instance = FluentHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fluent_handler_instance.setFormatter(formatter)
        logger.addHandler(fluent_handler_instance)

    return logger
