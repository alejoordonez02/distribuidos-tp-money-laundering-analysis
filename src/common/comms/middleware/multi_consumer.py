import logging
from typing import Callable, NamedTuple

from pika import BlockingConnection, ConnectionParameters
from pika.exceptions import AMQPConnectionError

from .errors import MOMDisconnectedError

MessageHandler = Callable[[bytes, Callable[[], None], Callable[[], None]], None]


class _Source(NamedTuple):
    queue_name: str
    handler: MessageHandler
    prefetch: int


class MultiQueueConsumer:
    """Consumes several queues on a single connection and thread, routing each
    message to the handler registered for its queue.

    A node uses this to drive its data input and its EOF-ring control queue from
    one consume loop, so every state mutation happens on one thread and the
    checkpoint stays atomic — no cross-thread races between data, ring and emission.

    Each queue gets its own prefetch window (per-consumer QoS), so a data consumer
    that holds acks for a checkpoint batch does not starve the rare ring messages.
    """

    def __init__(self, host: str):
        self._conn = BlockingConnection(ConnectionParameters(host, heartbeat=0))
        self._chan = self._conn.channel()
        self._sources: list[_Source] = []

    def add_queue(
        self,
        queue_name: str,
        handler: MessageHandler,
        prefetch: int = 1,
        durable: bool = False,
        exchange: str | None = None,
        routing_key: str | None = None,
    ):
        # durable + exchange-bound queues survive a crash and keep accumulating
        # (redelivering un-acked messages) until the node returns.
        self._chan.queue_declare(queue=queue_name, durable=durable)
        if exchange is not None:
            self._chan.exchange_declare(exchange=exchange)
            self._chan.queue_bind(
                exchange=exchange, queue=queue_name, routing_key=routing_key or ""
            )
        self._sources.append(_Source(queue_name, handler, prefetch))

    def _ack(self, method):
        self._conn.add_callback_threadsafe(
            lambda: self._chan.basic_ack(delivery_tag=method.delivery_tag)
        )

    def _nack(self, method):
        self._conn.add_callback_threadsafe(
            lambda: self._chan.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        )

    def start(self):
        for src in self._sources:
            # per-consumer prefetch: the data queue can hold a whole checkpoint batch
            # without blocking ring control delivery on the same channel.
            self._chan.basic_qos(prefetch_count=src.prefetch, global_qos=False)
            self._chan.basic_consume(
                queue=src.queue_name,
                on_message_callback=self._make_callback(src.handler),
            )
        try:
            self._chan.start_consuming()
        except AMQPConnectionError as e:
            raise MOMDisconnectedError(str(e)) from e
        except Exception as e:
            logging.error("unhandled exception in MultiQueueConsumer: %s", e, exc_info=True)

    def _make_callback(self, handler: MessageHandler):
        def callback(_chan, method, _props, body):
            handler(body, lambda: self._ack(method), lambda: self._nack(method))

        return callback

    def stop(self):
        try:
            self._chan.stop_consuming()
        except Exception as e:
            logging.error("unhandled exception stopping MultiQueueConsumer: %s", e)

    def send(self, queue_name: str, message: bytes):
        self._chan.basic_publish(exchange="", routing_key=queue_name, body=message)

    def close(self):
        try:
            self._conn.close()
        except Exception as e:
            logging.error("unhandled exception closing MultiQueueConsumer: %s", e)
