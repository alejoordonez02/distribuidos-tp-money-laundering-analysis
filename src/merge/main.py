import logging
import os

from merge import Merge
from merge_fns import UC2BankIdMergeFn, UC3BankIdMergeFn

from common.comms.middleware import QueueRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
LEFT_RX = os.environ["LEFT_RX"]
RIGHT_RX = os.environ["RIGHT_RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    left_rx = QueueRabbitMQ(MOM_HOST, LEFT_RX)
    right_rx = QueueRabbitMQ(MOM_HOST, RIGHT_RX)

    match STRATEGY:
        case "uc2_merge":
            fn = UC2BankIdMergeFn()
        case "uc3_merge":
            fn = UC3BankIdMergeFn()
        case _:
            raise ValueError(f"unknown group_by strategy: {STRATEGY}")

    Merge(left_rx, right_rx, fn, MOM_HOST, TX).start()


if __name__ == "__main__":
    main()
