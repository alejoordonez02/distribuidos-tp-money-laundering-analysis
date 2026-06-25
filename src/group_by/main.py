import logging
import os

from group_by_fns import (
    GroupByFn,
    UC2BankNamesGroupByFn,
    UC2MaxAmountGroupByFn,
    UC3SumGroupByFn,
    UC4ComputeGraph,
    UC5ConverterGroupByFn,
    UC5CountGroupByFn,
)
from strategies import GroupByStrategy

from common.checkpoint import make_checkpointer
from common.conversion import FrankfurterConversionAPI
from common.heartbeat import run_with_heartbeat
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.middleware import MultiQueueConsumer, RingRabbitMQ, make_txs
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
    routes: list[tuple[str, int]],
    mom_host: str,
    rx_name: str,
) -> RingGroupBy:
    """Build a group-by node. `routes` is one (tx_name, naffinity_downstream) per
    downstream fleet — a single route for the common one-aggregate case, several to fan
    one build out to distinct aggregates (UC4). Each fleet gets its own stamping producer
    id, so downstream dedup stays per-route."""
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

    return RingGroupBy(
        fn,
        idx,
        rc,
        sent,
        consumer,
        ring,
        fleets,
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

    # UC4 fans the graph to two aggregates (full graph + degree filter); others use one route
    routes = [(TX, NAFFINITY_DOWNSTREAM)]

    match STRATEGY:
        case GroupByStrategy.UC2_MAX_AMOUNT:
            fn = UC2MaxAmountGroupByFn()
        case GroupByStrategy.UC2_BANK_NAMES:
            fn = UC2BankNamesGroupByFn()
        case GroupByStrategy.UC3_SUM:
            fn = UC3SumGroupByFn()
        case GroupByStrategy.UC4_COMPUTE_GRAPH:
            fn = UC4ComputeGraph()
            routes.append((os.environ["TX_DEGREE"], int(os.environ["NAFFINITY_DEGREE"])))
        case GroupByStrategy.UC5_CONVERTER:
            fn = UC5ConverterGroupByFn(FrankfurterConversionAPI())
        case GroupByStrategy.UC5_COUNT:
            fn = UC5CountGroupByFn()
        case _:
            raise ValueError(f"unknown group_by strategy: {STRATEGY}")

    groupby = make_groupby(fn, IDX, routes, MOM_HOST, RX)

    logging.info("starting groupby: fn=%s idx=%s rx=%s routes=%s", type(fn), IDX, RX, routes)
    run_with_heartbeat(groupby.start)


if __name__ == "__main__":
    main()
