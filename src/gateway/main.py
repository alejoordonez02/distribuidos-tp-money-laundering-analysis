import logging
import os
from socket import AF_INET, SOCK_STREAM, socket
from uuid import UUID

from pika import BlockingConnection, ConnectionParameters

from common.comms.middleware import ExchangeRabbitMQ
from common.heartbeat import run_with_heartbeat
from gateway import Gateway

GATEWAY_HOST = os.environ["GATEWAY_HOST"]
GATEWAY_PORT = os.environ["GATEWAY_PORT"]
MOM_HOST = os.environ["MOM_HOST"]
RESPONSES_EXCHANGE = os.environ["SERVER_QUEUE_RX"]
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
    """A tx factory: one exchange shard publisher per downstream ring peer (routing
    key = shard idx). A single-peer ring (n == 1) is still exchange-routed — its one
    consumer reads `{exchange}0` exactly as when scaled — so the gateway never needs
    a plain-queue special case (which would miss the ring's exchange-bound shard)."""
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


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    listener = socket(AF_INET, SOCK_STREAM)
    addr = (GATEWAY_HOST, int(GATEWAY_PORT))

    def responses_rx_factory(client_id: UUID) -> ExchangeRabbitMQ:
        """Build a client's own response consumer, bound by its client_id to the
        responses exchange. The queue is exclusive so it auto-deletes when the client
        disconnects, cleaning up its broker resources without any explicit teardown."""
        return ExchangeRabbitMQ(
            MOM_HOST,
            RESPONSES_EXCHANGE,
            routing_keys=[str(client_id)],
            queue_name=f"{RESPONSES_EXCHANGE}.{client_id}",
            exclusive=True,
        )

    trans_tx_factory = _make_factory(TRANSACTIONS_TX, NDEFAULT_FILTERS)
    accs_tx_factory = _make_factory(ACCOUNTS_TX, NBANK_NAMES)

    gateway = Gateway(
        listener, addr, responses_rx_factory, trans_tx_factory, accs_tx_factory
    )
    run_with_heartbeat(gateway.start)


if __name__ == "__main__":
    main()
