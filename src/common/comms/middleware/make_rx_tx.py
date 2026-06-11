from typing import Optional

from .exchange_rabbitmq import ExchangeRabbitMQ
from .queue_rabbitmq import QueueRabbitMQ
from .stamping_mom import (
    DerivedStampingMOM,
    InputContext,
    StampingMOM,
    derive_producer_id,
)


def make_rx_tx(
    idx: int,
    rx_name: str,
    tx_name: str,
    mom_host: str,
    naffinity_downstream: int,
    affinity_upstream: bool,
    durable_rx: bool = False,
    rx_prefetch: int = 1,
    derived_stamping: bool = False,
):
    # derived_stamping: same output id whichever peer processed the input (competing-safe)
    rx = _make_rx(idx, mom_host, rx_name, affinity_upstream, durable_rx, rx_prefetch)
    ctx = InputContext() if derived_stamping else None
    tx = _make_tx(idx, mom_host, tx_name, naffinity_downstream, ctx)

    return rx, tx, ctx


def _make_rx(
    idx: int,
    mom_host: str,
    rx_name: str,
    affinity_upstream: bool,
    durable_rx: bool,
    rx_prefetch: int,
):
    # durable_rx survives consumer crashes; rx_prefetch must cover the checkpoint batch
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


def _stamp(inner, tx_name: str, idx: int, route: int, ctx: Optional[InputContext]):
    if ctx is not None:
        return DerivedStampingMOM(inner, ctx)
    return StampingMOM(inner, derive_producer_id(tx_name, idx, route))


def _make_tx(
    idx: int,
    mom_host: str,
    tx_name: str,
    naffinity_downstream: int,
    ctx: Optional[InputContext],
):
    if naffinity_downstream < 0:
        raise ValueError("downstream nodes amount cannot be less than 0")

    if naffinity_downstream == 0:
        inner = QueueRabbitMQ(mom_host, queue_name=f"{tx_name}")
        return (_stamp(inner, tx_name, idx, 0, ctx),)

    return [
        _stamp(
            ExchangeRabbitMQ(
                mom_host, tx_name, routing_keys=[f"{n}"], queue_name=f"{tx_name}{n}"
            ),
            tx_name,
            idx,
            n,
            ctx,
        )
        for n in range(naffinity_downstream)
    ]
