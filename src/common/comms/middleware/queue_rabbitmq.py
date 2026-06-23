import logging
from typing import Callable

from pika import BlockingConnection, ConnectionParameters
from pika.exceptions import AMQPConnectionError, ConnectionWrongStateError

from .errors import (
    MOMDisconnectedError,
    MOMMessageError,
)
from .queue_mom import MOMQueue


class QueueRabbitMQ(MOMQueue):
    def __init__(self, host: str, queue_name: str, prefetch_count: int = 1):
        self.host = host
        self.queue_name = queue_name
        # Must be >= the checkpoint batch size, otherwise holding acks for a batch
        # deadlocks against the broker (it won't deliver past the prefetch window).
        self.prefetch_count = prefetch_count

        # TODO: heartbeat=600 may be starved by blocking callbacks in start_consuming — revisit
        self.conn = BlockingConnection(ConnectionParameters(host, heartbeat=0))
        self.chan = self.conn.channel()
        self.chan.queue_declare(queue=queue_name)
        self.chan.basic_qos(prefetch_count=prefetch_count)

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
            # TODO: handle specific pika shutdown exceptions
            logging.error(
                "!!! UNHANDLED exception in stop_consuming (queue=%s): %s",
                self.queue_name,
                e,
                exc_info=True,
            )

    def send(self, message: bytes, routing_key: str | None = None) -> None:
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
            # TODO: handle specific close-on-wrong-state case
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
        # Schedule on the connection's own thread: acks may be flushed (batched
        # checkpointing) from a different thread than the one owning this channel
        # (e.g. the merge's two side threads), and pika is not thread-safe.
        self.conn.add_callback_threadsafe(
            lambda: chan.basic_ack(delivery_tag=method.delivery_tag)
        )

    def _nack(self, chan, method) -> None:
        self.conn.add_callback_threadsafe(
            lambda: chan.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        )
