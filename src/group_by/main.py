import logging
import os

from group_by_fns import (
    GroupByFn,
    UC2BankNamesGroupByFn,
    UC2MaxAmountGroupByFn,
    UC3SumGroupByFn,
    UC4ComputeGraph,
    UC5CountGroupByFn,
)
from strategies import GroupByStrategy

from common.checkpoint import make_checkpointer
from common.heartbeat import run_with_heartbeat
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.middleware import MultiQueueConsumer, RingRabbitMQ, make_txs
from ring_broadcast_group_by import RingBroadcastGroupBy
from ring_group_by import RingGroupBy

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

IDX = int(os.getenv("IDX", 0))
NAFFINITY_DOWNSTREAM = int(os.environ["NAFFINITY_DOWNSTREAM"])

STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def make_groupby(
    fn: GroupByFn,
    idx: int,
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
) -> RingGroupBy:
    npeers = int(os.getenv("NPEERS", "1"))
    external_txs = make_txs(idx, tx_name, mom_host, naffinities_downstream)

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
        extra_state={"completion": rc, "sent": sent},
    )

    return RingGroupBy(
        fn,
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


def make_broadcast_groupby(
    fn: GroupByFn,
    idx: int,
    routes: list[tuple[str, int]],
    mom_host: str,
    rx_name: str,
) -> RingBroadcastGroupBy:
    """Build a group-by that fans its partials out to several downstream fleets. `routes`
    is one (tx_name, naffinity_downstream) per fleet; each gets its own stamping producer
    id, so downstream dedup stays per-route just like with separate builders."""
    npeers = int(os.getenv("NPEERS", "1"))
    fleets = [make_txs(idx, tx, mom_host, naffinity) for tx, naffinity in routes]
    external_txs = [tx for fleet in fleets for tx in fleet]

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
        extra_state={"completion": rc, "sent": sent},
    )

    return RingBroadcastGroupBy(
        fn,
        fleets,
        idx,
        rc,
        sent,
        consumer,
        ring,
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

    # UC4 builds the transaction graph once and fans it out to two aggregates (the full
    # graph and the degree filter), so it routes through the broadcast group-by instead.
    if STRATEGY == GroupByStrategy.UC4_COMPUTE_GRAPH:
        routes = [
            (TX, NAFFINITY_DOWNSTREAM),
            (os.environ["TX_DEGREE"], int(os.environ["NAFFINITY_DEGREE"])),
        ]
        groupby = make_broadcast_groupby(UC4ComputeGraph(), IDX, routes, MOM_HOST, RX)
        logging.info("starting broadcast groupby: idx=%s rx=%s routes=%s", IDX, RX, routes)
        run_with_heartbeat(groupby.start)
        return

    match STRATEGY:
        case GroupByStrategy.UC2_MAX_AMOUNT:
            fn = UC2MaxAmountGroupByFn()
        case GroupByStrategy.UC2_BANK_NAMES:
            fn = UC2BankNamesGroupByFn()
        case GroupByStrategy.UC3_SUM:
            fn = UC3SumGroupByFn()
        case GroupByStrategy.UC5_COUNT:
            fn = UC5CountGroupByFn()
        case _:
            raise ValueError(f"unknown group_by strategy: {STRATEGY}")

    groupby = make_groupby(fn, IDX, NAFFINITY_DOWNSTREAM, MOM_HOST, RX, TX)

    logging.info("starting groupby: fn=%s idx=%s rx=%s tx=%s", type(fn), IDX, RX, TX)
    run_with_heartbeat(groupby.start)


if __name__ == "__main__":
    main()
