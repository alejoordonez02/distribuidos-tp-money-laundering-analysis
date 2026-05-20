import logging
import os

from aggregate import Aggregate
from aggregate_fns import UC2BankNamesAggregateFn, UC2MaxAmountAggregateFn

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
            fn = UC2MaxAmountAggregateFn()
        case "uc2_bank_names":
            fn = UC2BankNamesAggregateFn()
        case _:
            raise ValueError(f"unknown aggregate strategy: {STRATEGY}")

    Aggregate(QueueRabbitMQ(MOM_HOST, RX), fn, QueueRabbitMQ(MOM_HOST, TX)).start()


if __name__ == "__main__":
    main()
