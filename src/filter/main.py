import logging
import os
from typing import Callable

from filter_fns import FilterFn, UC3AvgFilter, UC4PathFilter, UC5AmountFilter
from ring_broadcast_filter import RingBroadcastFilter
from ring_filter import RingFilter
from strategies import FilterStrategy

from common.checkpoint import make_checkpointer
from common.heartbeat import run_with_heartbeat
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.middleware import (
    MultiQueueConsumer,
    RingRabbitMQ,
    make_txs,
)

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
STRATEGY = os.environ["STRATEGY"]

STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def make_default_filter() -> RingBroadcastFilter:
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

    # Sharded by message identity so a post-crash re-emit lands on the same peer's dedup
    route_table = [
        (os.environ["UC1_TRANSACTIONS_TX"], UC1Filter(), "UC1_TRANSACTIONS_SHARDS"),
        (os.environ["UC2_TRANSACTIONS_TX"], UC2Filter(), "UC2_TRANSACTIONS_SHARDS"),
        (os.environ["UC3_PERIOD_A_TRANSACTIONS_TX"], UC3FilterPeriodA(), "UC3_PERIOD_A_SHARDS"),
        (os.environ["UC4_TRANSACTIONS_TX"], UC4Filter(), "UC4_TRANSACTIONS_SHARDS"),
        (os.environ["UC5_TRANSACTIONS_TX"], UC5Filter(), "UC5_TRANSACTIONS_SHARDS"),
        (os.environ["UC3_PERIOD_B_TRANSACTIONS_TX"], UC3FilterPeriodB(), "UC3_PERIOD_B_SHARDS"),
    ]
    # naffinity 0 = plain work queue (UC1 -> join); >= 1 = one exchange shard per ring peer
    routes, sharded_routes = [], []
    for tx, filter_fn, shards_env in route_table:
        naffinity = int(os.getenv(shards_env, "1"))
        txs = make_txs(idx, tx, MOM_HOST, naffinity)
        if naffinity == 0:
            routes.append((txs[0], filter_fn))
        else:
            sharded_routes.append((list(txs), filter_fn))

    ring_name = os.environ["RING_NAME"]
    peer_ids = [i for i in range(npeers) if i != idx]
    rc = RingCompletion(idx, peer_ids)
    sent = SentCounts()
    consumer = MultiQueueConsumer(MOM_HOST)
    ring = RingRabbitMQ(MOM_HOST, ring_name, idx, peer_ids)

    external_txs = [tx for tx, _ in routes] + [tx for shards, _ in sharded_routes for tx in shards]
    checkpointer = make_checkpointer(
        STATE_DIR, f"{STRATEGY}_{idx}", external_txs, CHECKPOINT_EVERY,
        extra_state={"completion": rc, "sent": sent},
    )

    return RingBroadcastFilter(
        routes,
        sharded_routes,
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
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
) -> RingFilter:
    npeers = int(os.getenv("NPEERS", "1"))
    external_txs = make_txs(idx, tx_name, mom_host, naffinities_downstream)

    ring_name = os.environ["RING_NAME"]
    peer_ids = [i for i in range(npeers) if i != idx]
    rc = RingCompletion(idx, peer_ids)
    sent = SentCounts()
    consumer = MultiQueueConsumer(mom_host)
    ring = RingRabbitMQ(mom_host, ring_name, idx, peer_ids)

    checkpointer = make_checkpointer(
        STATE_DIR, f"{STRATEGY}_{idx}", external_txs, CHECKPOINT_EVERY,
        extra_state={"completion": rc, "sent": sent},
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
            return run_with_heartbeat(make_default_filter().start)
        case FilterStrategy.UC3_AVG:
            fn = UC3AvgFilter
        case FilterStrategy.UC4_PATH:
            fn = UC4PathFilter
        case FilterStrategy.UC5_AMOUNT:
            fn = UC5AmountFilter
        case _:
            raise ValueError(f"unknown filter strategy: {STRATEGY}")

    idx = int(os.getenv("IDX", 0))
    naffinity_downstream = int(os.environ["NAFFINITY_DOWNSTREAM"])
    run_with_heartbeat(
        make_filter(fn, idx, naffinity_downstream, MOM_HOST, RX, os.environ["TX"]).start
    )


if __name__ == "__main__":
    main()
