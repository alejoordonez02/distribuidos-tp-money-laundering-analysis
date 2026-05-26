from typing import Callable

from pika import BlockingConnection, ConnectionParameters
from pika.exceptions import AMQPConnectionError

from .errors import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)
from .middleware import MessageMiddlewareExchange, MessageMiddlewareQueue


class QueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host: str, queue_name: str):
        self.host = host
        self.queue_name = queue_name
        self.conn = BlockingConnection(ConnectionParameters(host))
        self.chan = self.conn.channel()
        self.chan.queue_declare(queue=queue_name)

    def start_consuming(
        self, on_message_callback: Callable[[bytes, Callable, Callable], None]
    ) -> None:
        def callback(chan, method, _properties, body):
            on_message_callback(
                body, lambda: self._ack(chan, method), lambda: self._nack(chan, method)
            )

        self.chan.basic_consume(queue=self.queue_name, on_message_callback=callback)
        try:
            self.chan.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def stop_consuming(self) -> None:
        try:
            self.chan.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e

    def send(self, message: bytes) -> None:
        try:
            self.chan.basic_publish(
                exchange="", routing_key=self.queue_name, body=message
            )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def clone(self) -> "QueueRabbitMQ":
        return QueueRabbitMQ(self.host, self.queue_name)

    def _ack(self, chan, method) -> None:
        chan.basic_ack(delivery_tag=method.delivery_tag)

    def _nack(self, chan, method) -> None:
        chan.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


class ExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(
        self, host: str, exchange_name: str, routing_keys: list[str], exchange_type: str = "direct"
    ):
        self.host = host
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.routing_keys = routing_keys
        self.conn = BlockingConnection(ConnectionParameters(host))
        self.chan = self.conn.channel()
        self.chan.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

        queue = self.chan.queue_declare(queue="", exclusive=True)
        queue_name = queue.method.queue
        self.queue_name = queue_name
        for k in routing_keys:
            self.chan.queue_bind(
                exchange=exchange_name, queue=queue_name, routing_key=k
            )

    def start_consuming(
        self, on_message_callback: Callable[[bytes, Callable, Callable], None]
    ) -> None:
        def callback(chan, method, _properties, body):
            on_message_callback(
                body, lambda: self._ack(chan, method), lambda: self._nack(chan, method)
            )

        self.chan.basic_consume(queue=self.queue_name, on_message_callback=callback)
        try:
            self.chan.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def stop_consuming(self) -> None:
        try:
            self.chan.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e

    def send(self, message: bytes) -> None:
        for k in self.routing_keys:
            try:
                self.chan.basic_publish(
                    exchange=self.exchange_name, routing_key=k, body=message
                )
            except AMQPConnectionError as e:
                raise MessageMiddlewareDisconnectedError(str(e)) from e
            except Exception as e:
                raise MessageMiddlewareMessageError(str(e)) from e

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def clone(self) -> "ExchangeRabbitMQ":
        return ExchangeRabbitMQ(self.host, self.exchange_name, self.routing_keys, self.exchange_type)

    def _ack(self, chan, method) -> None:
        chan.basic_ack(delivery_tag=method.delivery_tag)

    def _nack(self, chan, method) -> None:
        chan.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
