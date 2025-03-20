import pika
import json
import logging

logger = logging.getLogger(__name__)


class RabbitMQPublisher:
    def __init__(self, host='localhost', port=5672, user='guest', password='guest', exchange='metrics'): # this is a nono but whatever
        self.connection_params = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=pika.PlainCredentials(user, password)
        )
        self.exchange = exchange
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(self.connection_params)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='topic', durable=True)
            logger.info(f"Connected to RabbitMQ and declared exchange: {self.exchange}")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            self.connection = None
            self.channel = None

    def publish(self, routing_key, message):
        """Publish a message to RabbitMQ"""
        if self.channel is None:
            self.connect()
            if self.channel is None:
                logger.error("Cannot publish message: no connection to RabbitMQ")
                return False

        try:
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=1, # swap to 2 if there isn't alot of metrics or use a ttl lol
                    content_type='application/json'
                )
            )
            return True
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            self.connection = None
            self.channel = None
            return False

    def close(self):
        """Close the connection to RabbitMQ"""
        if self.connection and self.connection.is_open:
            self.connection.close()


# make sure we're using one connection so we don't have to create a new connection each time
_publisher = None


def get_publisher():
    global _publisher
    if _publisher is None:
        _publisher = RabbitMQPublisher()
    return _publisher


def publish_metrics(service_name, metric_type, data):
    """Publish metrics to RabbitMQ"""
    publisher = get_publisher()
    routing_key = f"{service_name}.{metric_type}"
    publisher.publish(routing_key, data)