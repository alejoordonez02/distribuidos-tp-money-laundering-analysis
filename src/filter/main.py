import logging
import os
from typing import Callable

from filter2 import Filter
from filter_fns import FilterFn, UC3AvgFilter, UC4PathFilter, UC5AmountFilter

from aggregate.main import AggregateStrategy
from common.comms.eof_handler import make_stateless_eof_handler
from common.comms.middleware import QueueRabbitMQ, make_rx_tx

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
STRATEGY = os.environ["STRATEGY"]
# This reading half varies depending on the controller instance that's being
# used, thus it does not need to be declared for each strategy.

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def make_default_filter() -> Filter:
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
    UC4_DEGREE_TRANSACTIONS_TX = os.environ["UC4_DEGREE_TRANSACTIONS_TX"]
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
        (QueueRabbitMQ(MOM_HOST, UC4_DEGREE_TRANSACTIONS_TX), UC4Filter()),
        (QueueRabbitMQ(MOM_HOST, UC5_TRANSACTIONS_TX), UC5Filter()),
    ]

    eof_handler = make_stateless_eof_handler(MOM_HOST, [tx for (tx, _) in routes])

    filter2 = Filter(transactions_rx, routes, eof_handler)

    return filter2


def make_filter(
    fn_factory: Callable[[], FilterFn],
    idx: int,
    affinity_upstream: bool,
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
) -> Filter:

    external_rx, external_txs = make_rx_tx(
        idx, rx_name, tx_name, mom_host, naffinities_downstream, affinity_upstream
    )

    eof_handler = make_stateless_eof_handler(MOM_HOST, (external_txs[0],))

    filter2 = Filter(
        external_rx, [(tx, fn_factory()) for tx in external_txs], eof_handler
    )

    return filter2


from enum import StrEnum


class FilterStrategy(StrEnum):
    DEFAULT = "default"
    UC3_AVG = "uc3_avg"
    UC4_PATH = "uc4_path"
    UC5_AMOUNT = "uc5_amount"


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case FilterStrategy.DEFAULT:
            filter2 = make_default_filter()
            return filter2.start()

        case FilterStrategy.UC3_AVG:
            fn = UC3AvgFilter
        case FilterStrategy.UC4_PATH:
            fn = UC4PathFilter
        case FilterStrategy.UC5_AMOUNT:
            fn = UC5AmountFilter
        case _:
            raise ValueError(f"unknown filter strategy: {STRATEGY}")

    TX = os.environ["TX"]

    IDX = int(os.getenv("IDX", 0))
    AFFINITY_UPSTREAM = os.environ["AFFINITY_UPSTREAM"] == "1"
    NAFFINITY_DOWNSTREAM = int(os.environ["NAFFINITY_DOWNSTREAM"])

    filter2 = make_filter(
        fn, IDX, AFFINITY_UPSTREAM, NAFFINITY_DOWNSTREAM, MOM_HOST, RX, TX
    )

    logging.info(
        f"""
        starting filter: fn={type(fn)}, \
        idx={IDX}, affinity_upstream={AFFINITY_UPSTREAM}, \
        nnodes_downstream={NAFFINITY_DOWNSTREAM}, \
        mom_host={MOM_HOST}, rx={RX}, tx={TX}"\
        """
    )
    filter2.start()


if __name__ == "__main__":
    main()
