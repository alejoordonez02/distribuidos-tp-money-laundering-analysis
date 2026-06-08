import logging
import os

from merge_fns import UC2BankIdMergeFn, UC3BankIdMergeFn, UC4PruneMergeFn
from strategies import MergeStrategy

from common.comms.middleware import QueueRabbitMQ
from merge import Merge

MOM_HOST = os.environ["MOM_HOST"]
LEFT_RX = os.environ["LEFT_RX"]
RIGHT_RX = os.environ["RIGHT_RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    left_rx = QueueRabbitMQ(MOM_HOST, LEFT_RX)
    right_rx = QueueRabbitMQ(MOM_HOST, RIGHT_RX)

    def tx_factory():
        return QueueRabbitMQ(MOM_HOST, TX)

    match STRATEGY:
        case MergeStrategy.UC2_MERGE:
            fn = UC2BankIdMergeFn()
        case MergeStrategy.UC3_MERGE:
            fn = UC3BankIdMergeFn()
        case MergeStrategy.UC4_PRUNE:
            fn = UC4PruneMergeFn()
        case _:
            raise ValueError(f"unknown group_by strategy: {STRATEGY}")

    Merge(left_rx, right_rx, fn, tx_factory).start()


if __name__ == "__main__":
    main()
