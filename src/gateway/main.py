import logging
import os
from socket import AF_INET, SOCK_STREAM, socket

from pika import BlockingConnection, ConnectionParameters

from common.comms.middleware import (
    ExchangeRabbitMQ,
    QueueRabbitMQ,
)
from gateway import Gateway

GATEWAY_HOST = os.environ["GATEWAY_HOST"]
GATEWAY_PORT = os.environ["GATEWAY_PORT"]
MOM_HOST = os.environ["MOM_HOST"]
SERVER_QUEUE_RX = os.environ["SERVER_QUEUE_RX"]
TRANSACTIONS_TX = os.environ["TRANSACTIONS_TX"]
ACCOUNTS_TX = os.environ["ACCOUNTS_TX"]
# downstream rings consume their input by affinity: the gateway routes each message
# to one of N durable shard queues bound to the *_TX exchange (1 = a plain work queue).
NDEFAULT_FILTERS = int(os.getenv("NDEFAULT_FILTERS", "1"))
NBANK_NAMES = int(os.getenv("NBANK_NAMES", "1"))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def _declare_shards(host: str, exchange: str, n: int):
    """Pre-declare the durable shard queues + bindings so no message is lost if the
    gateway publishes before a downstream peer has bound its queue. Idempotent with
    the peer's own declare (same durable, same exchange/routing key)."""
    conn = BlockingConnection(ConnectionParameters(host, heartbeat=0))
    chan = conn.channel()
    chan.exchange_declare(exchange=exchange)
    for i in range(n):
        chan.queue_declare(queue=f"{exchange}{i}", durable=True)
        chan.queue_bind(exchange=exchange, queue=f"{exchange}{i}", routing_key=str(i))
    conn.close()


def _make_factory(exchange: str, n: int):
    """A tx factory: N exchange shard publishers (routing key = shard idx) when scaled
    to an affinity ring, or a single working queue when n == 1."""
    if n > 1:
        _declare_shards(MOM_HOST, exchange, n)

        def factory():
            return [
                ExchangeRabbitMQ(
                    MOM_HOST,
                    exchange,
                    routing_keys=[str(i)],
                    queue_name=f"{exchange}{i}",
                    exclusive=False,
                )
                for i in range(n)
            ]

        return factory

    def factory():
        return [QueueRabbitMQ(MOM_HOST, exchange)]

    return factory


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    listener = socket(AF_INET, SOCK_STREAM)
    addr = (GATEWAY_HOST, int(GATEWAY_PORT))
    server_rx = QueueRabbitMQ(MOM_HOST, SERVER_QUEUE_RX)

    trans_tx_factory = _make_factory(TRANSACTIONS_TX, NDEFAULT_FILTERS)
    accs_tx_factory = _make_factory(ACCOUNTS_TX, NBANK_NAMES)

    gateway = Gateway(listener, addr, server_rx, trans_tx_factory, accs_tx_factory)
    gateway.start()


if __name__ == "__main__":
    main()
