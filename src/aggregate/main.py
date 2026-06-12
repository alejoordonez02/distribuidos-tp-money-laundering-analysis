import logging
import os
from queue import Queue

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

from aggregate import Aggregate
from ring_aggregate import RingAggregate
from common.checkpoint import MultiShardSpill, make_checkpointer
from common.comms.eof_handler import make_stateful_eof_handler
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.messages import EOF
from common.comms.middleware import MultiQueueConsumer, RingRabbitMQ, make_rx_tx

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

IDX = int(os.getenv("IDX", 0))
AFFINITY_UPSTREAM = os.environ["AFFINITY_UPSTREAM"] == "1"
NAFFINITY_DOWNSTREAM = int(os.getenv("NAFFINITY_DOWNSTREAM", 0))
# broadcast a small global result to every downstream replica (UC3 averages fanned
# out to the broadcast-join merges) instead of sharding by affinity.
BROADCAST_DOWNSTREAM = os.getenv("BROADCAST_DOWNSTREAM", "0") == "1"

STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def make_aggregate(
    fn: AggregateFn,
    idx: int,
    affinity_upstream: bool,
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
) -> "Aggregate | RingAggregate":

    npeers = int(os.getenv("NPEERS", "1"))
    if npeers > 1:
        return _make_ring_aggregate(
            fn, idx, naffinities_downstream, mom_host, rx_name, tx_name, npeers
        )

    external_rx, external_txs, _ = make_rx_tx(
        idx,
        rx_name,
        tx_name,
        mom_host,
        naffinities_downstream,
        affinity_upstream,
        durable_rx=STATE_DIR is not None,
        rx_prefetch=CHECKPOINT_EVERY if STATE_DIR else 1,
    )

    internal_eofs = Queue[EOF]()
    eof_handler = make_stateful_eof_handler(MOM_HOST, external_txs, internal_eofs)

    checkpointer = make_checkpointer(
        STATE_DIR,
        f"{STRATEGY}_{idx}",
        external_txs,
        CHECKPOINT_EVERY,
        fn,
        extra_state={"eof": eof_handler},
    )

    return Aggregate(
        fn,
        external_rx,
        external_txs,
        eof_handler,
        internal_eofs,
        checkpointer,
        broadcast_downstream=BROADCAST_DOWNSTREAM,
    )


def _make_ring_aggregate(
    fn: AggregateFn,
    idx: int,
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
    npeers: int,
) -> RingAggregate:
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
    consumer = MultiQueueConsumer(mom_host)
    ring = RingRabbitMQ(mom_host, ring_name, idx, peer_ids)

    checkpointer = make_checkpointer(
        STATE_DIR,
        f"{STRATEGY}_{idx}",
        external_txs,
        CHECKPOINT_EVERY,
        fn,
        extra_state={"eof": rc},
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

    aggregate = make_aggregate(
        fn, IDX, AFFINITY_UPSTREAM, NAFFINITY_DOWNSTREAM, MOM_HOST, RX, TX
    )

    logging.info(
        f"""
        starting aggregate: fn={type(fn)}, \
        idx={IDX}, nnodes_downstream={NAFFINITY_DOWNSTREAM}, \
        mom_host={MOM_HOST}, rx={RX}, tx={TX}"\
        """
    )
    aggregate.start()


if __name__ == "__main__":
    main()
