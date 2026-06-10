from .exchange_rabbitmq import ExchangeRabbitMQ
from .queue_rabbitmq import QueueRabbitMQ
from .stamping_mom import StampingMOM, derive_producer_id


def make_rx_tx(
    idx: int,
    rx_name: str,
    tx_name: str,
    mom_host: str,
    naffinity_downstream: int,
    affinity_upstream: bool,
    durable_rx: bool = False,
    rx_prefetch: int = 1,
):
    rx = _make_rx(idx, mom_host, rx_name, affinity_upstream, durable_rx, rx_prefetch)
    tx = _make_tx(idx, mom_host, tx_name, naffinity_downstream)

    return rx, tx


def _make_rx(
    idx: int,
    mom_host: str,
    rx_name: str,
    affinity_upstream: bool,
    durable_rx: bool,
    rx_prefetch: int,
):
    # durable_rx keeps the queue across consumer crashes (needed for crash recovery).
    # rx_prefetch must cover the checkpoint batch so held acks don't deadlock.
    return (
        ExchangeRabbitMQ(
            mom_host,
            rx_name,
            [f"{idx}"],
            f"{rx_name}{idx}",
            exclusive=not durable_rx,
            prefetch_count=rx_prefetch,
        )
        if affinity_upstream
        else QueueRabbitMQ(mom_host, rx_name, prefetch_count=rx_prefetch)
    )


def _make_tx(idx: int, mom_host: str, tx_name: str, naffinity_downstream: int):
    # Wrap each tx so every data message it emits carries a producer_id + seq.
    if naffinity_downstream < 0:
        raise ValueError("downstream nodes amount cannot be less than 0")

    if naffinity_downstream == 0:
        inner = QueueRabbitMQ(mom_host, queue_name=f"{tx_name}")
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
