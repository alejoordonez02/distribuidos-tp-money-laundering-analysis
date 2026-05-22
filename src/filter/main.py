import logging
import os

from filter2 import Filter

from common.comms.middleware import QueueRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
TRANSACTIONS_RX = os.environ["TRANSACTIONS_RX"]

STRATEGY = os.getenv("STRATEGY", "default")

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def make_default_filter():
    from filter_fns import (
        UC1Filter,
        UC2Filter,
        UC4Filter,
        UC5Filter,
    )

    UC1_TRANSACTIONS_TX = os.environ["FILTERED_TX"]
    UC2_TRANSACTIONS_TX = os.environ["UC2_TRANSACTIONS_TX"]
    UC4_TRANSACTIONS_TX = os.environ["UC4_TRANSACTIONS_TX"]
    UC5_TRANSACTIONS_TX = os.environ["UC5_TRANSACTIONS_TX"]

    transactions_rx = QueueRabbitMQ(MOM_HOST, TRANSACTIONS_RX)
    routes = [
        (QueueRabbitMQ(MOM_HOST, UC1_TRANSACTIONS_TX), UC1Filter()),
        (QueueRabbitMQ(MOM_HOST, UC2_TRANSACTIONS_TX), UC2Filter()),
        (QueueRabbitMQ(MOM_HOST, UC5_TRANSACTIONS_TX), UC5Filter()),
        (QueueRabbitMQ(MOM_HOST, UC4_TRANSACTIONS_TX), UC4Filter()),
    ]

    return Filter(transactions_rx, routes)


def make_uc5_amount_filter():
    from filter_fns import UC5AmountFilter

    UC5_AMOUNT_FILTERED_TX = os.environ["UC5_AMOUNT_FILTERED_TX"]

    transactions_rx = QueueRabbitMQ(MOM_HOST, TRANSACTIONS_RX)
    routes = [
        (QueueRabbitMQ(MOM_HOST, UC5_AMOUNT_FILTERED_TX), UC5AmountFilter()),
    ]

    return Filter(transactions_rx, routes)  # type: ignore[reportArgumentType]


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case "default":
            filter2 = make_default_filter()
        case "uc5_amount":
            filter2 = make_uc5_amount_filter()
        case _:
            raise ValueError(f"unknown filter strategy: {STRATEGY}")

    filter2.start()


if __name__ == "__main__":
    main()
