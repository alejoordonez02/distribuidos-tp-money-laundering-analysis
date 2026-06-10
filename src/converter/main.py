import logging
import os

from converter_fns import UC5USDConverterFn

from common.checkpoint import make_checkpointer
from common.comms.middleware import QueueRabbitMQ, StampingMOM, derive_producer_id
from common.conversion import FrankfurterConversionAPI
from converter import Converter

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.getenv("STRATEGY", "converter")

IDX = int(os.getenv("IDX", 0))
STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    tx = StampingMOM(QueueRabbitMQ(MOM_HOST, TX), derive_producer_id(TX, IDX, 0))
    checkpointer = make_checkpointer(
        STATE_DIR, f"{STRATEGY}_{IDX}", [tx], CHECKPOINT_EVERY
    )

    # The rx prefetch must cover the checkpoint batch so held acks don't deadlock.
    prefetch = CHECKPOINT_EVERY if STATE_DIR else 1
    rx = QueueRabbitMQ(MOM_HOST, RX, prefetch_count=prefetch)

    Converter(
        rx,
        UC5USDConverterFn(FrankfurterConversionAPI()),
        tx,
        checkpointer,
    ).start()


if __name__ == "__main__":
    main()
