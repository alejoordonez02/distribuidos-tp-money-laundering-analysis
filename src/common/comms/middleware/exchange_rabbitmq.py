import logging
from typing import Callable

from pika import BlockingConnection, ConnectionParameters
from pika.exceptions import AMQPConnectionError, ConnectionWrongStateError

from .errors import (
    MOMDisconnectedError,
    MOMMessageError,
)
from .exchange_mom import MOMExchange

# Max messages RabbitMQ delivers without an ack. Keeps the socket write buffer
# from filling up when callbacks block on heavy processing (e.g. uc4 count paths).
PREFETCH_COUNT = 10


class ExchangeRabbitMQ(MOMExchange):
    def __init__(
        self,
        host: str,
        exchange_name: str,
        routing_keys: list[str],
        queue_name: str,
        exclusive: bool = True,
        prefetch_count: int = PREFETCH_COUNT,
    ):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = queue_name
        # Non-exclusive queues survive a consumer crash so RabbitMQ keeps
        # accumulating (and redelivers un-acked) messages until the node returns.
        self.exclusive = exclusive
        # Must be >= the checkpoint batch, otherwise holding acks for a batch
        # deadlocks against the broker's delivery window.
        self.prefetch_count = max(prefetch_count, PREFETCH_COUNT)

        # TODO: heartbeat=600 may be starved by blocking callbacks in start_consuming — revisit
        self.conn = BlockingConnection(ConnectionParameters(host, heartbeat=0))
        self.chan = self.conn.channel()
        self.chan.exchange_declare(exchange=exchange_name)
        self.chan.confirm_delivery()

    def start_consuming(
        self, on_message_callback: Callable[[bytes, Callable, Callable], None]
    ) -> None:

        self.chan.queue_declare(
            queue=self.queue_name, exclusive=self.exclusive, durable=not self.exclusive
        )
        for k in self.routing_keys:
            self.chan.queue_bind(
                exchange=self.exchange_name, queue=self.queue_name, routing_key=k
            )

        # Limit unacked messages so a slow consumer doesn't let RabbitMQ fill the
        # socket write buffer (which closes the connection with send_failed,timeout).
        self.chan.basic_qos(prefetch_count=self.prefetch_count)

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

    def send(self, message: bytes, routing_key: str | None = None) -> None:
        keys = [routing_key] if routing_key is not None else self.routing_keys
        for k in keys:
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
            self.host,
            self.exchange_name,
            self.routing_keys,
            self.queue_name,
            self.exclusive,
            self.prefetch_count,
        )

    def _ack(self, chan, method) -> None:
        # Schedule on the connection's own thread: acks may be flushed (batched
        # checkpointing) from a different thread than the one owning this channel,
        # and pika is not thread-safe.
        self.conn.add_callback_threadsafe(
            lambda: chan.basic_ack(delivery_tag=method.delivery_tag)
        )

    def _nack(self, chan, method) -> None:
        self.conn.add_callback_threadsafe(
            lambda: chan.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        )
