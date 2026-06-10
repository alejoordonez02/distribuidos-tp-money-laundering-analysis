import logging
import os

from join_fns import UC1Join, UC2Join, UC3Join, UC4Join, UC5Join

from common.checkpoint import PersistentSpill
from common.comms.middleware import QueueRabbitMQ
from join import Join

MOM_HOST = os.environ["MOM_HOST"]
UC1_RX = os.environ["UC1_RX"]
UC2_RX = os.environ["UC2_RX"]
UC3_RX = os.environ["UC3_RX"]
UC4_RX = os.environ["UC4_RX"]
UC5_RX = os.environ["UC5_RX"]
RESPONSES_TX = os.environ["RESPONSES_TX"]

STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    spill_dir = os.path.join(STATE_DIR or "/tmp", "spill")
    prefetch = CHECKPOINT_EVERY if STATE_DIR else 1

    def rx(name: str):
        return lambda: QueueRabbitMQ(MOM_HOST, name, prefetch_count=prefetch)

    partial_res_handlers = [
        (rx(UC1_RX), UC1Join(PersistentSpill(spill_dir, "uc1_join")), 1),
        (rx(UC2_RX), UC2Join(), 2),
        (rx(UC3_RX), UC3Join(PersistentSpill(spill_dir, "uc3_join")), 3),
        (rx(UC4_RX), UC4Join(), 4),
        (rx(UC5_RX), UC5Join(), 5),
    ]

    def responses_tx_factory():
        return QueueRabbitMQ(MOM_HOST, RESPONSES_TX)

    Join(
        partial_res_handlers, responses_tx_factory, STATE_DIR, CHECKPOINT_EVERY
    ).start()


if __name__ == "__main__":
    main()
