import logging
import os
from typing import Callable

from filter2 import Filter
from filter_fns import FilterFn, UC3AvgFilter, UC4PathFilter, UC5AmountFilter
from strategies import FilterStrategy

from common.checkpoint import make_checkpointer
from common.comms.eof_handler import make_stateless_eof_handler
from common.comms.middleware import (
    DerivedStampingMOM,
    InputContext,
    QueueRabbitMQ,
    make_rx_tx,
)

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
STRATEGY = os.environ["STRATEGY"]

STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def make_default_filter() -> Filter:
    from filter_fns import (
        UC1Filter,
        UC2Filter,
        UC3FilterPeriodA,
        UC3FilterPeriodB,
        UC4Filter,
        UC5Filter,
    )

    idx = int(os.getenv("IDX", 0))

    # competing input: derived stamping for crash-stable output ids
    input_ctx = InputContext()

    route_specs = [
        (os.environ["UC1_TRANSACTIONS_TX"], UC1Filter()),
        (os.environ["UC2_TRANSACTIONS_TX"], UC2Filter()),
        (os.environ["UC3_PERIOD_A_TRANSACTIONS_TX"], UC3FilterPeriodA()),
        (os.environ["UC3_PERIOD_B_TRANSACTIONS_TX"], UC3FilterPeriodB()),
        (os.environ["UC4_TRANSACTIONS_TX"], UC4Filter()),
        (os.environ["UC4_DEGREE_TRANSACTIONS_TX"], UC4Filter()),
        (os.environ["UC5_TRANSACTIONS_TX"], UC5Filter()),
    ]
    routes = [
        (DerivedStampingMOM(QueueRabbitMQ(MOM_HOST, queue), input_ctx), fn)
        for queue, fn in route_specs
    ]

    prefetch = CHECKPOINT_EVERY if STATE_DIR else 1
    transactions_rx = QueueRabbitMQ(MOM_HOST, RX, prefetch_count=prefetch)

    txs = [tx for tx, _ in routes]
    eof_handler = make_stateless_eof_handler(MOM_HOST, txs)
    checkpointer = make_checkpointer(
        STATE_DIR, f"{STRATEGY}_{idx}", (), CHECKPOINT_EVERY,
        extra_state={"eof": eof_handler},
    )

    return Filter(transactions_rx, routes, eof_handler, checkpointer, input_ctx)


def make_filter(
    fn_factory: Callable[[], FilterFn],
    idx: int,
    affinity_upstream: bool,
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
) -> Filter:

    external_rx, external_txs, input_ctx = make_rx_tx(
        idx,
        rx_name,
        tx_name,
        mom_host,
        naffinities_downstream,
        affinity_upstream,
        durable_rx=STATE_DIR is not None,
        rx_prefetch=CHECKPOINT_EVERY if STATE_DIR else 1,
        derived_stamping=not affinity_upstream,
    )

    eof_handler = make_stateless_eof_handler(MOM_HOST, (external_txs[0],))
    checkpointer = make_checkpointer(
        STATE_DIR, f"{STRATEGY}_{idx}",
        () if input_ctx else external_txs, CHECKPOINT_EVERY,
        extra_state={"eof": eof_handler},
    )

    return Filter(
        external_rx,
        [(tx, fn_factory()) for tx in external_txs],
        eof_handler,
        checkpointer,
        input_ctx,
    )


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case FilterStrategy.DEFAULT:
            return make_default_filter().start()

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

    filter2.start()


if __name__ == "__main__":
    main()
