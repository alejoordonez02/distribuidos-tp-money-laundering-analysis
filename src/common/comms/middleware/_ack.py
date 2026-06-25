"""Thread-safe ack/nack helpers shared by the middleware consumers.

FT-critical: acks may be flushed from another thread (e.g. merge's side
threads) and pika is NOT thread-safe, so the basic_ack/basic_nack call must be
scheduled on the connection's own thread via add_callback_threadsafe.

Callers differ only in where the connection and channel come from:
- QueueRabbitMQ / ExchangeRabbitMQ pass the channel handed to their consume
  callback (self.conn, callback's chan).
- MultiQueueConsumer uses its instance channel (self._conn, self._chan).
The conn, chan, method (and nack's requeue) are parametrized so the semantics
stay identical for every caller.
"""


def ack_threadsafe(conn, chan, method) -> None:
    # Schedule on the connection's thread: acks may be flushed from another
    # thread (e.g. merge's side threads) and pika isn't thread-safe.
    conn.add_callback_threadsafe(
        lambda: chan.basic_ack(delivery_tag=method.delivery_tag)
    )


def nack_threadsafe(conn, chan, method, requeue: bool = True) -> None:
    conn.add_callback_threadsafe(
        lambda: chan.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)
    )
