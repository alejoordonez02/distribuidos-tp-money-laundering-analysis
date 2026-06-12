import logging
import os
from typing import Callable

from filter2 import Filter
from filter_fns import FilterFn, UC3AvgFilter, UC4PathFilter, UC5AmountFilter
from ring_filter import RingFilter, SentCounts
from strategies import FilterStrategy

from common.checkpoint import make_checkpointer
from common.comms.eof_handler import make_stateless_eof_handler
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.middleware import (
    DerivedStampingMOM,
    ExchangeRabbitMQ,
    InputContext,
    MultiQueueConsumer,
    QueueRabbitMQ,
    RingRabbitMQ,
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

    # broadcast routes: every transaction reaches each UC's queue
    route_specs = [
        (os.environ["UC1_TRANSACTIONS_TX"], UC1Filter()),
        (os.environ["UC2_TRANSACTIONS_TX"], UC2Filter()),
        (os.environ["UC3_PERIOD_A_TRANSACTIONS_TX"], UC3FilterPeriodA()),
        (os.environ["UC4_TRANSACTIONS_TX"], UC4Filter()),
        (os.environ["UC4_DEGREE_TRANSACTIONS_TX"], UC4Filter()),
        (os.environ["UC5_TRANSACTIONS_TX"], UC5Filter()),
    ]
    routes = [
        (DerivedStampingMOM(QueueRabbitMQ(MOM_HOST, queue), input_ctx), fn)
        for queue, fn in route_specs
    ]

    # period-B feeds the UC3 broadcast-join merges; shard it across N when scaled
    # (opt-in via UC3_PERIOD_B_SHARDS), otherwise keep the single broadcast queue.
    pb = os.environ["UC3_PERIOD_B_TRANSACTIONS_TX"]
    n_pb = int(os.getenv("UC3_PERIOD_B_SHARDS", "1"))
    sharded_routes = []
    if n_pb > 1:
        pb_shards = [
            DerivedStampingMOM(
                ExchangeRabbitMQ(
                    MOM_HOST, pb, routing_keys=[f"{i}"], queue_name=f"{pb}{i}"
                ),
                input_ctx,
            )
            for i in range(n_pb)
        ]
        sharded_routes = [(pb_shards, UC3FilterPeriodB())]
    else:
        routes.append(
            (DerivedStampingMOM(QueueRabbitMQ(MOM_HOST, pb), input_ctx), UC3FilterPeriodB())
        )

    prefetch = CHECKPOINT_EVERY if STATE_DIR else 1
    transactions_rx = QueueRabbitMQ(MOM_HOST, RX, prefetch_count=prefetch)

    txs = [tx for tx, _ in routes] + [tx for shards, _ in sharded_routes for tx in shards]
    eof_handler = make_stateless_eof_handler(MOM_HOST, txs)
    checkpointer = make_checkpointer(
        STATE_DIR, f"{STRATEGY}_{idx}", (), CHECKPOINT_EVERY,
        extra_state={"eof": eof_handler},
    )

    return Filter(
        transactions_rx, routes, eof_handler, checkpointer, input_ctx,
        sharded_routes=sharded_routes,
    )


def make_filter(
    fn_factory: Callable[[], FilterFn],
    idx: int,
    affinity_upstream: bool,
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
) -> "Filter | RingFilter":

    npeers = int(os.getenv("NPEERS", "1"))
    if npeers > 1 and affinity_upstream:
        # affinity ring: per-peer completion on a single consume thread (data + ring),
        # so a crash restores a consistent shard + barrier phase. Only for affinity
        # inputs; a competing filter (affinity_upstream=False) still uses the
        # working-queue path below until its upstream is converted to route by affinity.
        return _make_ring_filter(
            fn_factory, idx, naffinities_downstream, mom_host, rx_name, tx_name, npeers
        )

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

    eof_handler = make_stateless_eof_handler(MOM_HOST, external_txs)
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


def _make_ring_filter(
    fn_factory: Callable[[], FilterFn],
    idx: int,
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
    npeers: int,
) -> RingFilter:
    # the MultiQueueConsumer owns the data-shard consumption, so the make_rx_tx rx is
    # only used to build the downstream txs; close its idle connection.
    external_rx, external_txs, _ = make_rx_tx(
        idx,
        rx_name,
        tx_name,
        mom_host,
        naffinities_downstream,
        affinity_upstream=True,
        durable_rx=STATE_DIR is not None,
        rx_prefetch=CHECKPOINT_EVERY if STATE_DIR else 1,
    )
    external_rx.close()

    ring_name = os.environ["RING_NAME"]
    peer_ids = [i for i in range(npeers) if i != idx]
    rc = RingCompletion(idx, peer_ids)
    sent = SentCounts()
    consumer = MultiQueueConsumer(mom_host)
    ring = RingRabbitMQ(mom_host, ring_name, idx, peer_ids)

    checkpointer = make_checkpointer(
        STATE_DIR,
        f"{STRATEGY}_{idx}",
        external_txs,
        CHECKPOINT_EVERY,
        extra_state={"eof": rc, "sent": sent},
    )

    return RingFilter(
        fn_factory(),
        idx,
        rc,
        sent,
        consumer,
        ring,
        external_txs,
        data_queue=f"{rx_name}{idx}",
        data_exchange=rx_name,
        ring_queue=f"{ring_name}_queue{idx}",
        ring_exchange=ring_name,
        data_prefetch=max(CHECKPOINT_EVERY, 10),
        checkpointer=checkpointer,
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
