import logging
import os

from group_by import GroupBy
from group_by_fns import UC2BankNamesGroupByFn, UC2MaxAmountGroupByFn, UC3SumGroupByFn

from common.comms.middleware import QueueRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case "uc2_max_amount":
            fn = UC2MaxAmountGroupByFn()
        case "uc2_bank_names":
            fn = UC2BankNamesGroupByFn()
        case "uc3_sum":
            fn = UC3SumGroupByFn()
        case _:
            raise ValueError(f"unknown group_by strategy: {STRATEGY}")

    GroupBy(QueueRabbitMQ(MOM_HOST, RX), fn, QueueRabbitMQ(MOM_HOST, TX)).start()


if __name__ == "__main__":
    main()
