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
# default filters consume their transactions by affinity: the gateway routes each
# transaction to one of N durable shard queues bound to the TRANSACTIONS_TX exchange.
NDEFAULT_FILTERS = int(os.getenv("NDEFAULT_FILTERS", "1"))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def _declare_transaction_shards(host: str, exchange: str, n: int):
    """Pre-declare the durable shard queues + bindings so no transaction is lost if
    the gateway publishes before a default filter has bound its queue. Idempotent
    with the default filter's own declare (same durable, same exchange/routing key)."""
    conn = BlockingConnection(ConnectionParameters(host, heartbeat=0))
    chan = conn.channel()
    chan.exchange_declare(exchange=exchange)
    for i in range(n):
        chan.queue_declare(queue=f"{exchange}{i}", durable=True)
        chan.queue_bind(exchange=exchange, queue=f"{exchange}{i}", routing_key=str(i))
    conn.close()


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    listener = socket(AF_INET, SOCK_STREAM)
    addr = (GATEWAY_HOST, int(GATEWAY_PORT))
    server_rx = QueueRabbitMQ(MOM_HOST, SERVER_QUEUE_RX)

    affinity = NDEFAULT_FILTERS > 1
    if affinity:
        _declare_transaction_shards(MOM_HOST, TRANSACTIONS_TX, NDEFAULT_FILTERS)

    def trans_tx_factory():
        # affinity: one publisher per shard (routing key = shard idx) on the
        # TRANSACTIONS_TX exchange. competing fallback: a single working queue.
        if affinity:
            return [
                ExchangeRabbitMQ(
                    MOM_HOST,
                    TRANSACTIONS_TX,
                    routing_keys=[str(i)],
                    queue_name=f"{TRANSACTIONS_TX}{i}",
                    exclusive=False,
                )
                for i in range(NDEFAULT_FILTERS)
            ]
        return [QueueRabbitMQ(MOM_HOST, TRANSACTIONS_TX)]

    def accs_tx_factory():
        return QueueRabbitMQ(MOM_HOST, ACCOUNTS_TX)

    gateway = Gateway(listener, addr, server_rx, trans_tx_factory, accs_tx_factory)
    gateway.start()


if __name__ == "__main__":
    main()
