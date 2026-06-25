import logging
from typing import Callable

from pika import BlockingConnection, ConnectionParameters
from pika.exceptions import AMQPConnectionError, ConnectionWrongStateError

from ._ack import ack_threadsafe, nack_threadsafe
from .errors import (
    MOMDisconnectedError,
    MOMMessageError,
)
from .queue_mom import MOMQueue


class QueueRabbitMQ(MOMQueue):
    def __init__(self, host: str, queue_name: str, prefetch_count: int = 1):
        self.host = host
        self.queue_name = queue_name
        # Must be >= the checkpoint batch size, else holding acks for a batch deadlocks the broker's prefetch window.
        self.prefetch_count = prefetch_count

        self.conn = BlockingConnection(ConnectionParameters(host, heartbeat=0))
        self.chan = self.conn.channel()
        self.chan.queue_declare(queue=queue_name)
        self.chan.basic_qos(prefetch_count=prefetch_count)
        self.chan.confirm_delivery()

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
            logging.error(
                "!!! UNHANDLED exception in start_consuming (queue=%s): %s",
                self.queue_name,
                e,
                exc_info=True,
            )

    def stop_consuming(self) -> None:
        try:
            self.chan.stop_consuming()
        except AMQPConnectionError as e:
            raise MOMDisconnectedError(str(e)) from e
        except Exception as e:
            logging.error(
                "!!! UNHANDLED exception in stop_consuming (queue=%s): %s",
                self.queue_name,
                e,
                exc_info=True,
            )

    def send(self, message: bytes) -> None:
        try:
            self.chan.basic_publish(
                exchange="", routing_key=self.queue_name, body=message
            )
        except AMQPConnectionError as e:
            raise MOMDisconnectedError(str(e)) from e
        except Exception as e:
            raise MOMMessageError(str(e)) from e

    def close(self) -> None:
        try:
            self.conn.close()
        except ConnectionWrongStateError as e:
            logging.error(
                "!!! UNHANDLED ConnectionWrongStateError in close (queue=%s): %s",
                self.queue_name,
                e,
                exc_info=True,
            )
            
        except Exception as e:
            raise MOMMessageError(str(e)) from e

    def clone(self) -> "QueueRabbitMQ":
        return QueueRabbitMQ(self.host, self.queue_name, self.prefetch_count)

    def _ack(self, chan, method) -> None:
        ack_threadsafe(self.conn, chan, method)

    def _nack(self, chan, method) -> None:
        nack_threadsafe(self.conn, chan, method)
