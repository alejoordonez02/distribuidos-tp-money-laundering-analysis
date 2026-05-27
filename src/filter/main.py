import logging
import os

from eof_handler import EOFHandler
from filter2 import Filter
from filter_fns import FilterFn
from ring_eof_handler import RingEOFHandler

from common.comms.middleware import MOMQueue, MOMRing, QueueRabbitMQ, RingRabbitMQ

IDX = int(os.getenv("IDX", "0"))
NPEERS = int(os.getenv("NPEERS", "0"))
RING_NAME: str = os.getenv("RING_NAME", "")

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
# This reading half varies depending on the controller instance that's being
# used, thus it does not need to be declared for each strategy.

STRATEGY = os.getenv("STRATEGY", "default")

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def make_default_filter() -> tuple[MOMQueue, list[tuple[MOMQueue, FilterFn]]]:
    from filter_fns import (
        UC1Filter,
        UC2Filter,
        UC3FilterPeriodA,
        UC3FilterPeriodB,
        UC4Filter,
        UC5Filter,
    )

    UC1_TRANSACTIONS_TX = os.environ["UC1_TRANSACTIONS_TX"]
    UC2_TRANSACTIONS_TX = os.environ["UC2_TRANSACTIONS_TX"]
    UC3_PERIOD_A_TRANSACTIONS_TX = os.environ["UC3_PERIOD_A_TRANSACTIONS_TX"]
    UC3_PERIOD_B_TRANSACTIONS_TX = os.environ["UC3_PERIOD_B_TRANSACTIONS_TX"]
    UC4_TRANSACTIONS_TX = os.environ["UC4_TRANSACTIONS_TX"]
    UC5_TRANSACTIONS_TX = os.environ["UC5_TRANSACTIONS_TX"]

    transactions_rx = QueueRabbitMQ(MOM_HOST, RX)
    routes = [
        (QueueRabbitMQ(MOM_HOST, UC1_TRANSACTIONS_TX), UC1Filter()),
        (QueueRabbitMQ(MOM_HOST, UC2_TRANSACTIONS_TX), UC2Filter()),
        (
            QueueRabbitMQ(MOM_HOST, UC3_PERIOD_A_TRANSACTIONS_TX),
            UC3FilterPeriodA(),
        ),
        (
            QueueRabbitMQ(MOM_HOST, UC3_PERIOD_B_TRANSACTIONS_TX),
            UC3FilterPeriodB(),
        ),
        (QueueRabbitMQ(MOM_HOST, UC4_TRANSACTIONS_TX), UC4Filter()),
        (QueueRabbitMQ(MOM_HOST, UC5_TRANSACTIONS_TX), UC5Filter()),
    ]

    return transactions_rx, routes


def make_uc3_average_filter() -> tuple[MOMQueue, list[tuple[MOMQueue, FilterFn]]]:
    from filter_fns import UC3AvgFilter

    UC3_FILTERED_TX = os.environ["UC3_FILTERED_TX"]

    transactions_rx = QueueRabbitMQ(MOM_HOST, RX)
    routes = [(QueueRabbitMQ(MOM_HOST, UC3_FILTERED_TX), UC3AvgFilter())]

    return transactions_rx, routes  # type: ignore[reportReturnType]


def make_uc4_path_filter() -> tuple[MOMQueue, list[tuple[MOMQueue, FilterFn]]]:
    from filter_fns import UC4PathFilter

    UC4_FILTERED_PATHS_TX = os.environ["UC4_FILTERED_PATHS_TX"]

    transactions_rx = QueueRabbitMQ(MOM_HOST, RX)
    routes = [
        (QueueRabbitMQ(MOM_HOST, UC4_FILTERED_PATHS_TX), UC4PathFilter()),
    ]

    return transactions_rx, routes  # type: ignore[reportReturnType]


def make_uc5_amount_filter() -> tuple[MOMQueue, list[tuple[MOMQueue, FilterFn]]]:
    from filter_fns import UC5AmountFilter

    UC5_AMOUNT_FILTERED_TX = os.environ["UC5_AMOUNT_FILTERED_TX"]

    transactions_rx = QueueRabbitMQ(MOM_HOST, RX)
    routes = [
        (QueueRabbitMQ(MOM_HOST, UC5_AMOUNT_FILTERED_TX), UC5AmountFilter()),
    ]

    return transactions_rx, routes  # type: ignore[reportReturnType]


def make_eof_handler(txs: list[MOMQueue]) -> EOFHandler:
    peer_ids = [idx for idx in range(NPEERS) if idx != IDX]
    mom_ring = RingRabbitMQ(MOM_HOST, RING_NAME, IDX, peer_ids)
    eof_handler = RingEOFHandler(mom_ring, txs)

    return eof_handler


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case "default":
            transactions_rx, routes = make_default_filter()
        case "uc3_avg":
            transactions_rx, routes = make_uc3_average_filter()
        case "uc4_path":
            transactions_rx, routes = make_uc4_path_filter()
        case "uc5_amount":
            transactions_rx, routes = make_uc5_amount_filter()
        case _:
            raise ValueError(f"unknown filter strategy: {STRATEGY}")

    eof_handler = make_eof_handler([tx for (tx, _) in routes])

    filter2 = Filter(transactions_rx, routes, eof_handler)
    filter2.start()


if __name__ == "__main__":
    main()
