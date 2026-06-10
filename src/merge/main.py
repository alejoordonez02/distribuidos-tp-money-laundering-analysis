import logging
import os

from merge_fns import UC2BankIdMergeFn, UC3BankIdMergeFn, UC4PruneMergeFn
from strategies import MergeStrategy

from common.checkpoint import make_checkpointer
from common.comms.middleware import (
    CounterSeqSource,
    QueueRabbitMQ,
    SeqCounter,
    StampingMOM,
    derive_producer_id,
)
from merge import Merge, MergeCounts

MOM_HOST = os.environ["MOM_HOST"]
LEFT_RX = os.environ["LEFT_RX"]
RIGHT_RX = os.environ["RIGHT_RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

IDX = int(os.getenv("IDX", 0))
STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case MergeStrategy.UC2_MERGE:
            fn = UC2BankIdMergeFn()
        case MergeStrategy.UC3_MERGE:
            fn = UC3BankIdMergeFn()
        case MergeStrategy.UC4_PRUNE:
            fn = UC4PruneMergeFn()
        case _:
            raise ValueError(f"unknown merge strategy: {STRATEGY}")

    # The result is emitted from whichever side completes the merge, so both side
    # txs share one stamping counter to keep the output sequence deterministic.
    out_counter = SeqCounter()
    producer_id = derive_producer_id(TX, IDX, 0)

    def tx_factory():
        return StampingMOM(QueueRabbitMQ(MOM_HOST, TX), producer_id, out_counter)

    prefetch = CHECKPOINT_EVERY if STATE_DIR else 1
    left_rx = QueueRabbitMQ(MOM_HOST, LEFT_RX, prefetch_count=prefetch)
    right_rx = QueueRabbitMQ(MOM_HOST, RIGHT_RX, prefetch_count=prefetch)

    counts = MergeCounts()
    checkpointer = make_checkpointer(
        STATE_DIR,
        f"{STRATEGY}_{IDX}",
        [CounterSeqSource(producer_id, out_counter)],
        CHECKPOINT_EVERY,
        fn,
        extra_state={"counts": counts},
    )

    Merge(left_rx, right_rx, fn, tx_factory, counts, checkpointer).start()


if __name__ == "__main__":
    main()
