import logging
import os

from merge_fns import UC2BankIdMergeFn, UC3BankIdMergeFn, UC4PruneMergeFn
from strategies import MergeStrategy

from common.checkpoint import PersistentSpill, make_checkpointer
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.middleware import (
    CounterSeqSource,
    ExchangeRabbitMQ,
    MultiQueueConsumer,
    QueueRabbitMQ,
    RingRabbitMQ,
    SeqCounter,
    StampingMOM,
    derive_producer_id,
)
from merge import Merge, MergeCounts
from ring_merge import MergeEofCounts, RingMerge

MOM_HOST = os.environ["MOM_HOST"]
LEFT_RX = os.environ["LEFT_RX"]
RIGHT_RX = os.environ["RIGHT_RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

IDX = int(os.getenv("IDX", 0))
NPEERS = int(os.getenv("NPEERS", 1))
NAFFINITY_DOWNSTREAM = int(os.getenv("NAFFINITY_DOWNSTREAM", 0))
STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    spill_dir = os.path.join(STATE_DIR or "/tmp", "spill")

    match STRATEGY:
        case MergeStrategy.UC2_MERGE:
            fn = UC2BankIdMergeFn()
        case MergeStrategy.UC3_MERGE:
            fn = UC3BankIdMergeFn(PersistentSpill(spill_dir, "uc3_merge"))
        case MergeStrategy.UC4_PRUNE:
            fn = UC4PruneMergeFn(PersistentSpill(spill_dir, "uc4_prune"))
        case _:
            raise ValueError(f"unknown merge strategy: {STRATEGY}")

    # both side txs share one stamping counter for a deterministic output sequence
    out_counter = SeqCounter()
    producer_id = derive_producer_id(TX, IDX, 0)

    def make_txs():
        # naffinity_downstream 0 = single work queue; >0 = one affinity-routed tx per shard
        if NAFFINITY_DOWNSTREAM == 0:
            return [StampingMOM(QueueRabbitMQ(MOM_HOST, TX), producer_id, out_counter)]
        return [
            StampingMOM(
                ExchangeRabbitMQ(
                    MOM_HOST, TX, routing_keys=[f"{i}"], queue_name=f"{TX}{i}"
                ),
                producer_id,
                out_counter,
            )
            for i in range(NAFFINITY_DOWNSTREAM)
        ]

    if NPEERS > 1:
        # scaled broadcast-join: left is broadcast (full state per peer), right is
        # sharded; a ring barrier consolidates the per-peer outputs into one EOF.
        _start_ring_merge(fn, make_txs(), out_counter, producer_id)
        return

    counts = MergeCounts()
    checkpointer = make_checkpointer(
        STATE_DIR,
        f"{STRATEGY}_{IDX}",
        [CounterSeqSource(producer_id, out_counter)],
        CHECKPOINT_EVERY,
        fn,
        extra_state={"counts": counts},
    )

    prefetch = CHECKPOINT_EVERY if STATE_DIR else 1
    left_rx = QueueRabbitMQ(MOM_HOST, LEFT_RX, prefetch_count=prefetch)
    right_rx = QueueRabbitMQ(MOM_HOST, RIGHT_RX, prefetch_count=prefetch)
    Merge(left_rx, right_rx, fn, make_txs, counts, checkpointer).start()


def _start_ring_merge(fn, external_txs, out_counter, producer_id):
    ring_name = os.environ["RING_NAME"]
    peer_ids = [i for i in range(NPEERS) if i != IDX]
    rc = RingCompletion(IDX, peer_ids)
    counts = MergeEofCounts()
    consumer = MultiQueueConsumer(MOM_HOST)
    ring = RingRabbitMQ(MOM_HOST, ring_name, IDX, peer_ids)

    checkpointer = make_checkpointer(
        STATE_DIR,
        f"{STRATEGY}_{IDX}",
        [CounterSeqSource(producer_id, out_counter)],
        CHECKPOINT_EVERY,
        fn,
        extra_state={"eof": rc, "merge_eof": counts},
    )

    RingMerge(
        fn,
        IDX,
        rc,
        counts,
        consumer,
        ring,
        external_txs,
        left_queue=f"{LEFT_RX}{IDX}",
        left_exchange=LEFT_RX,
        right_queue=f"{RIGHT_RX}{IDX}",
        right_exchange=RIGHT_RX,
        ring_queue=f"{ring_name}_queue{IDX}",
        ring_exchange=ring_name,
        data_prefetch=max(CHECKPOINT_EVERY, 10),
        checkpointer=checkpointer,
    ).start()


if __name__ == "__main__":
    main()
