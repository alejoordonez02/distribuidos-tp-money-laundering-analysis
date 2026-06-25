import logging
import os

from aggregate_fns import (
    AggregateFn,
    UC2BankNamesAggregateFn,
    UC2MaxAmountAggregateFn,
    UC3AvgAggregateFn,
    UC4AggregateGraphs,
    UC4AggregatePaths,
    UC4CountPaths,
    UC4Degree,
)
from strategies import AggregateStrategy

from ring_aggregate import RingAggregate
from common.checkpoint import MultiShardSpill, make_checkpointer
from common.heartbeat import run_with_heartbeat
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.middleware import MultiQueueConsumer, RingRabbitMQ, make_txs

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

IDX = int(os.getenv("IDX", 0))
NAFFINITY_DOWNSTREAM = int(os.getenv("NAFFINITY_DOWNSTREAM", 0))
# broadcast a small global result to every downstream replica instead of sharding by affinity (UC3 averages to the broadcast-join merges)
BROADCAST_DOWNSTREAM = os.getenv("BROADCAST_DOWNSTREAM", "0") == "1"

STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def make_aggregate(
    fn: AggregateFn,
    idx: int,
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
) -> RingAggregate:
    npeers = int(os.getenv("NPEERS", "1"))
    external_txs = make_txs(idx, tx_name, mom_host, naffinities_downstream)

    ring_name = os.environ["RING_NAME"]
    peer_ids = [i for i in range(npeers) if i != idx]
    rc = RingCompletion(idx, peer_ids)
    consumer = MultiQueueConsumer(mom_host)
    ring = RingRabbitMQ(mom_host, ring_name, idx, peer_ids)

    checkpointer = make_checkpointer(
        STATE_DIR,
        f"{STRATEGY}_{idx}",
        external_txs,
        CHECKPOINT_EVERY,
        fn,
        extra_state={"completion": rc},
    )

    return RingAggregate(
        fn,
        idx,
        rc,
        consumer,
        ring,
        external_txs,
        data_queue=f"{rx_name}{idx}",
        data_exchange=rx_name,
        ring_queue=f"{ring_name}_queue{idx}",
        ring_exchange=ring_name,
        data_prefetch=max(CHECKPOINT_EVERY, 10),
        checkpointer=checkpointer,
        broadcast_downstream=BROADCAST_DOWNSTREAM,
    )


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    spill_dir = os.path.join(STATE_DIR or "/tmp", "spill")

    def spill(tag: str) -> MultiShardSpill:
        return MultiShardSpill(spill_dir, tag)

    match STRATEGY:
        case AggregateStrategy.UC2_MAX_AMOUNT:
            fn = UC2MaxAmountAggregateFn()
        case AggregateStrategy.UC2_BANK_NAMES:
            fn = UC2BankNamesAggregateFn()
        case AggregateStrategy.UC3_AVERAGE:
            fn = UC3AvgAggregateFn()
        case AggregateStrategy.UC4_COUNT_PATHS:
            fn = UC4CountPaths(spill("uc4_count_paths"))
        case AggregateStrategy.UC4_AGGREGATE_GRAPHS:
            fn = UC4AggregateGraphs(spill("uc4_aggregate_graphs"))
        case AggregateStrategy.UC4_PATHS:
            fn = UC4AggregatePaths(spill("uc4_aggregate_paths"))
        case AggregateStrategy.UC4_DEGREE:
            fn = UC4Degree(spill("uc4_degree"))
        case _:
            raise ValueError(f"unknown aggregate strategy: {STRATEGY}")

    aggregate = make_aggregate(fn, IDX, NAFFINITY_DOWNSTREAM, MOM_HOST, RX, TX)

    logging.info("starting aggregate: fn=%s idx=%s rx=%s tx=%s", type(fn), IDX, RX, TX)
    run_with_heartbeat(aggregate.start)


if __name__ == "__main__":
    main()
