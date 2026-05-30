import logging
from typing import Callable

from pika import BlockingConnection, ConnectionParameters
from pika.exceptions import AMQPConnectionError, ConnectionWrongStateError

from .errors import (
    MOMDisconnectedError,
    MOMMessageError,
)
from .exchange_mom import MOMExchange


class ExchangeRabbitMQ(MOMExchange):
    def __init__(
        self,
        host: str,
        exchange_name: str,
        routing_keys: list[str],
        queue_name: str,
    ):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        # TODO: heartbeat=600 may be starved by blocking callbacks in start_consuming — revisit
        self.conn = BlockingConnection(ConnectionParameters(host, heartbeat=600))
        self.chan = self.conn.channel()
        self.chan.exchange_declare(exchange=exchange_name)

        # NOTE: estaban quedando msjs en ready cuando miraba
        #       el monitor de rabbit, para debuggear les puse
        #       nombre pero cuando les puse nombre
        #       desaparecieron esos msjs... El problema es
        #       que para ponerles nombre tuve que bochar este
        #       `exclusive=True`, porque tendría que una sola
        #       vez, pero de ambos lados, prod consumer, se
        #       declara por si acaso.
        # queue = self.chan.queue_declare(queue="", exclusive=True)
        # queue_name = queue.method.queue
        self.chan.queue_declare(queue=queue_name)
        self.queue_name = queue_name
        for k in self.routing_keys:
            self.chan.queue_bind(
                exchange=self.exchange_name, queue=self.queue_name, routing_key=k
            )

    def start_consuming(
        self, on_message_callback: Callable[[bytes, Callable, Callable], None]
    ) -> None:
        def callback(chan, method, _, body):
            on_message_callback(
                body, lambda: self._ack(chan, method), lambda: self._nack(chan, method)
            )

        self.chan.basic_consume(queue=self.queue_name, on_message_callback=callback)
        try:
            self.chan.start_consuming()
        except AMQPConnectionError as e:
            raise MOMDisconnectedError(str(e)) from e
        except Exception as e:
            # TODO: distinguish OSError (socket closed mid-consume) from exceptions
            # raised inside on_message_callback — the latter would pass silently here
            logging.error(
                "!!! UNHANDLED exception in start_consuming (exchange=%s): %s",
                self.exchange_name,
                e,
                exc_info=True,
            )

    def stop_consuming(self) -> None:
        try:
            self.chan.stop_consuming()
        except AMQPConnectionError as e:
            raise MOMDisconnectedError(str(e)) from e
        except Exception as e:
            # TODO: handle specific pika shutdown exceptions
            logging.error(
                "!!! UNHANDLED exception in stop_consuming (exchange=%s): %s",
                self.exchange_name,
                e,
                exc_info=True,
            )

    def send(self, message: bytes) -> None:
        for k in self.routing_keys:
            try:
                self.chan.basic_publish(
                    exchange=self.exchange_name, routing_key=k, body=message
                )
            except AMQPConnectionError as e:
                raise MOMDisconnectedError(str(e)) from e
            except Exception as e:
                raise MOMMessageError(str(e)) from e

    def close(self) -> None:
        try:
            self.conn.close()
        except ConnectionWrongStateError as e:
            # TODO: handle specific close-on-wrong-state case
            logging.error(
                "!!! UNHANDLED ConnectionWrongStateError in close (exchange=%s): %s",
                self.exchange_name,
                e,
                exc_info=True,
            )
        except Exception as e:
            raise MOMMessageError(str(e)) from e

    def clone(self) -> "ExchangeRabbitMQ":
        return ExchangeRabbitMQ(
            self.host, self.exchange_name, self.routing_keys, self.queue_name
        )

    def _ack(self, chan, method) -> None:
        chan.basic_ack(delivery_tag=method.delivery_tag)

    def _nack(self, chan, method) -> None:
        chan.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
