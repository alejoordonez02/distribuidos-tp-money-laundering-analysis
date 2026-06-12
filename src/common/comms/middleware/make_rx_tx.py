from .exchange_rabbitmq import ExchangeRabbitMQ
from .queue_rabbitmq import QueueRabbitMQ
from .stamping_mom import StampingMOM, derive_producer_id


def make_txs(idx: int, tx_name: str, mom_host: str, naffinity_downstream: int):
    """Build a node's output writers, each stamping with a producer id unique to the
    (node, route) and a monotonic seq. naffinity_downstream 0 = a single work queue
    (the join's input); > 0 = one affinity-routed shard per downstream peer."""
    if naffinity_downstream < 0:
        raise ValueError("downstream nodes amount cannot be less than 0")

    if naffinity_downstream == 0:
        inner = QueueRabbitMQ(mom_host, queue_name=tx_name)
        return (StampingMOM(inner, derive_producer_id(tx_name, idx, 0)),)

    return [
        StampingMOM(
            ExchangeRabbitMQ(
                mom_host, tx_name, routing_keys=[f"{n}"], queue_name=f"{tx_name}{n}"
            ),
            derive_producer_id(tx_name, idx, n),
        )
        for n in range(naffinity_downstream)
    ]
