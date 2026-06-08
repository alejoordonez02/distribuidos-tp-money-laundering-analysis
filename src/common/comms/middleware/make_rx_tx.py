from .exchange_rabbitmq import ExchangeRabbitMQ
from .queue_rabbitmq import QueueRabbitMQ


def make_rx_tx(
    idx: int,
    rx_name: str,
    tx_name: str,
    mom_host: str,
    naffinity_downstream: int,
    affinity_upstream: bool,
):
    rx = _make_rx(idx, mom_host, rx_name, affinity_upstream)
    tx = _make_tx(mom_host, tx_name, naffinity_downstream)

    return rx, tx


def _make_rx(idx: int, mom_host: str, rx_name: str, affinity_upstream: bool):
    return (
        ExchangeRabbitMQ(mom_host, rx_name, [f"{idx}"], f"{rx_name}{idx}")
        if affinity_upstream
        else QueueRabbitMQ(mom_host, rx_name)
    )


def _make_tx(mom_host: str, tx_name: str, naffinity_downstream: int):
    if naffinity_downstream < 0:
        raise ValueError("downstream nodes amount cannot be less than 0")

    if naffinity_downstream == 0:
        return (QueueRabbitMQ(mom_host, queue_name=f"{tx_name}"),)
    elif naffinity_downstream == 1:
        return (QueueRabbitMQ(mom_host, queue_name=f"{tx_name}0"),)

    return [
        ExchangeRabbitMQ(
            mom_host, tx_name, routing_keys=[f"{n}"], queue_name=f"{tx_name}{n}"
        )
        for n in range(naffinity_downstream)
    ]
