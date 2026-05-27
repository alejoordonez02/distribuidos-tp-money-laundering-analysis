from typing import Callable

from pika import BlockingConnection, ConnectionParameters
from pika.exceptions import AMQPConnectionError, ConnectionWrongStateError

from .errors import (
    MOMDisconnectedError,
    MOMMessageError,
)
from .queue_mom import MOMQueue


class QueueRabbitMQ(MOMQueue):
    def __init__(self, host: str, queue_name: str):
        self.queue_name = queue_name
        # TODO: heartbeat=600 may be starved by blocking callbacks in start_consuming — revisit
        self.conn = BlockingConnection(ConnectionParameters(host, heartbeat=600))
        self.chan = self.conn.channel()
        self.chan.queue_declare(queue=queue_name)
        self.chan.basic_qos(prefetch_count=1)

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
        except Exception:
            pass  # pika internal error during graceful shutdown (close() called mid-consume)

    def stop_consuming(self) -> None:
        try:
            self.chan.stop_consuming()
        except AMQPConnectionError as e:
            raise MOMDisconnectedError(str(e)) from e
        except Exception:
            pass  # already stopped or connection closed

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
        except ConnectionWrongStateError:
            pass
        except Exception as e:
            raise MOMMessageError(str(e)) from e

    def _ack(self, chan, method) -> None:
        chan.basic_ack(delivery_tag=method.delivery_tag)

    def _nack(self, chan, method) -> None:
        chan.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
