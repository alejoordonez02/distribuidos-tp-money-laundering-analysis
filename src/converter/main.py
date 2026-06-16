import logging
import os

from converter_fns import UC5USDConverterFn

from common.checkpoint import make_checkpointer
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.middleware import (
    MultiQueueConsumer,
    RingRabbitMQ,
    make_txs,
)
from common.conversion import FrankfurterConversionAPI
from common.heartbeat import run_with_heartbeat
from ring_converter import RingConverter

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.getenv("STRATEGY", "converter")

IDX = int(os.getenv("IDX", 0))
NAFFINITY_DOWNSTREAM = int(os.getenv("NAFFINITY_DOWNSTREAM", "0"))
STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def make_converter() -> RingConverter:
    fn = UC5USDConverterFn(FrankfurterConversionAPI())
    npeers = int(os.getenv("NPEERS", "1"))
    external_txs = make_txs(IDX, TX, MOM_HOST, NAFFINITY_DOWNSTREAM)

    ring_name = os.environ["RING_NAME"]
    peer_ids = [i for i in range(npeers) if i != IDX]
    rc = RingCompletion(IDX, peer_ids)
    sent = SentCounts()
    consumer = MultiQueueConsumer(MOM_HOST)
    ring = RingRabbitMQ(MOM_HOST, ring_name, IDX, peer_ids)

    checkpointer = make_checkpointer(
        STATE_DIR,
        f"{STRATEGY}_{IDX}",
        external_txs,
        CHECKPOINT_EVERY,
        extra_state={"completion": rc, "sent": sent},
    )

    return RingConverter(
        fn,
        IDX,
        rc,
        sent,
        consumer,
        ring,
        external_txs,
        data_queue=f"{RX}{IDX}",
        data_exchange=RX,
        ring_queue=f"{ring_name}_queue{IDX}",
        ring_exchange=ring_name,
        data_prefetch=max(CHECKPOINT_EVERY, 10),
        checkpointer=checkpointer,
    )


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)
    run_with_heartbeat(make_converter().start)


if __name__ == "__main__":
    main()
