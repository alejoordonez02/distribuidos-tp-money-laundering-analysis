import logging
import os
from typing import Callable

from filter2 import Filter
from filter_fns import FilterFn, UC3AvgFilter, UC4PathFilter, UC5AmountFilter
from ring_broadcast_filter import RingBroadcastFilter
from ring_filter import RingFilter
from strategies import FilterStrategy

from common.checkpoint import make_checkpointer
from common.comms.eof_handler import make_stateless_eof_handler
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.sent_counts import SentCounts
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


def make_default_filter() -> "Filter | RingBroadcastFilter":
    from filter_fns import (
        UC1Filter,
        UC2Filter,
        UC3FilterPeriodA,
        UC3FilterPeriodB,
        UC4Filter,
        UC5Filter,
    )

    idx = int(os.getenv("IDX", 0))
    npeers = int(os.getenv("NPEERS", "1"))

    # outputs feed competing downstream stages, so stamp each with an id derived from
    # the input (DerivedStampingMOM) regardless of which peer produced it. In affinity
    # mode the same input always lands on the same shard, so a re-emit after a crash
    # derives the same id and dedups downstream.
    input_ctx = InputContext()

    # Every transaction is routed to each UC's pipeline. A route is BROADCAST to a
    # shared working queue (a competing downstream) by default, or SHARDED across N
    # affinity peers (opt-in via its *_SHARDS env) once that downstream is converted
    # to an affinity ring. Sharding by message identity makes a re-emit after a crash
    # land on the SAME downstream peer, so its per-shard dedup catches the duplicate —
    # the affinity consumer never double-counts it (a competing working queue would).
    route_table = [
        (os.environ["UC1_TRANSACTIONS_TX"], UC1Filter(), "UC1_TRANSACTIONS_SHARDS"),
        (os.environ["UC2_TRANSACTIONS_TX"], UC2Filter(), "UC2_TRANSACTIONS_SHARDS"),
        (os.environ["UC3_PERIOD_A_TRANSACTIONS_TX"], UC3FilterPeriodA(), "UC3_PERIOD_A_SHARDS"),
        (os.environ["UC4_TRANSACTIONS_TX"], UC4Filter(), "UC4_TRANSACTIONS_SHARDS"),
        (os.environ["UC4_DEGREE_TRANSACTIONS_TX"], UC4Filter(), "UC4_DEGREE_TRANSACTIONS_SHARDS"),
        (os.environ["UC5_TRANSACTIONS_TX"], UC5Filter(), "UC5_TRANSACTIONS_SHARDS"),
        (os.environ["UC3_PERIOD_B_TRANSACTIONS_TX"], UC3FilterPeriodB(), "UC3_PERIOD_B_SHARDS"),
    ]
    routes, sharded_routes = [], []
    for tx, filter_fn, shards_env in route_table:
        n = int(os.getenv(shards_env, "1"))
        if n > 1:
            shards = [
                DerivedStampingMOM(
                    ExchangeRabbitMQ(
                        MOM_HOST, tx, routing_keys=[f"{i}"], queue_name=f"{tx}{i}"
                    ),
                    input_ctx,
                )
                for i in range(n)
            ]
            sharded_routes.append((shards, filter_fn))
        else:
            routes.append(
                (DerivedStampingMOM(QueueRabbitMQ(MOM_HOST, tx), input_ctx), filter_fn)
            )

    if npeers > 1:
        # affinity ring: the gateway routes each transaction to this peer's durable
        # shard queue (RX{idx} bound to the RX exchange), so one thread consumes only
        # this slice (data + ring) and a crash restores a consistent shard + barrier.
        return _make_default_ring_filter(idx, npeers, input_ctx, routes, sharded_routes)

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


def _make_default_ring_filter(
    idx, npeers, input_ctx, routes, sharded_routes
) -> "RingBroadcastFilter":
    ring_name = os.environ["RING_NAME"]
    peer_ids = [i for i in range(npeers) if i != idx]
    rc = RingCompletion(idx, peer_ids)
    sent = SentCounts()
    consumer = MultiQueueConsumer(MOM_HOST)
    ring = RingRabbitMQ(MOM_HOST, ring_name, idx, peer_ids)

    # DerivedStampingMOM has no per-route seq to restore (the sub-index resets per
    # input), so only the dedup + ring + sent counters ride the checkpoint.
    checkpointer = make_checkpointer(
        STATE_DIR,
        f"{STRATEGY}_{idx}",
        (),
        CHECKPOINT_EVERY,
        extra_state={"eof": rc, "sent": sent},
    )

    return RingBroadcastFilter(
        routes,
        sharded_routes,
        input_ctx,
        idx,
        rc,
        sent,
        consumer,
        ring,
        data_queue=f"{RX}{idx}",
        data_exchange=RX,
        ring_queue=f"{ring_name}_queue{idx}",
        ring_exchange=ring_name,
        data_prefetch=max(CHECKPOINT_EVERY, 10),
        checkpointer=checkpointer,
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
