import logging
import os

from converter_fns import UC5USDConverterFn

from common.checkpoint import make_checkpointer
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.middleware import (
    DerivedStampingMOM,
    InputContext,
    MultiQueueConsumer,
    QueueRabbitMQ,
    RingRabbitMQ,
    make_rx_tx,
)
from common.conversion import FrankfurterConversionAPI
from converter import Converter
from ring_converter import RingConverter

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.getenv("STRATEGY", "converter")

IDX = int(os.getenv("IDX", 0))
STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def make_converter():
    fn = UC5USDConverterFn(FrankfurterConversionAPI())
    npeers = int(os.getenv("NPEERS", "1"))
    affinity_upstream = os.getenv("AFFINITY_UPSTREAM", "0") == "1"
    naffinity_downstream = int(os.getenv("NAFFINITY_DOWNSTREAM", "0"))

    if npeers > 1 and affinity_upstream:
        # affinity ring: per-peer crash-safe completion (data + ring on one thread).
        # Only for affinity inputs; a competing converter keeps the working-queue path.
        return _make_ring_converter(fn, IDX, naffinity_downstream, MOM_HOST, RX, TX, npeers)

    # The competing converter stamps outputs derived from the input (stable across
    # peers on a crash) and forwards to a single downstream work queue.
    input_ctx = InputContext()
    tx = DerivedStampingMOM(QueueRabbitMQ(MOM_HOST, TX), input_ctx)
    checkpointer = make_checkpointer(STATE_DIR, f"{STRATEGY}_{IDX}", (), CHECKPOINT_EVERY)
    prefetch = CHECKPOINT_EVERY if STATE_DIR else 1
    rx = QueueRabbitMQ(MOM_HOST, RX, prefetch_count=prefetch)
    return Converter(rx, fn, tx, checkpointer, input_ctx)


def _make_ring_converter(fn, idx, naffinity_downstream, mom_host, rx_name, tx_name, npeers):
    # the MultiQueueConsumer owns the data-shard consumption, so the make_rx_tx rx is
    # only used to build the downstream txs; close its idle connection.
    external_rx, external_txs, _ = make_rx_tx(
        idx,
        rx_name,
        tx_name,
        mom_host,
        naffinity_downstream,
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

    return RingConverter(
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


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)
    make_converter().start()


if __name__ == "__main__":
    main()
