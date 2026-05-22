import logging
import os

from filter2 import Filter
from filter_fns import UC1Filter, UC2Filter, UC5AmountFilter, UC5Filter

from common.comms.middleware import QueueRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
TRANSACTIONS_RX = os.environ["TRANSACTIONS_RX"]
STRATEGY = os.getenv("STRATEGY", "default")
FILTER_ID = int(os.environ["FILTER_ID"])
FILTER_WORKERS = int(os.environ["FILTER_WORKERS"])
FILTER_RING_BASE = os.environ["FILTER_RING_BASE"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    transactions_rx = QueueRabbitMQ(MOM_HOST, TRANSACTIONS_RX)
    ring_rx = QueueRabbitMQ(MOM_HOST, f"{FILTER_RING_BASE}_{FILTER_ID}")
    ring_tx = QueueRabbitMQ(MOM_HOST, f"{FILTER_RING_BASE}_{(FILTER_ID + 1) % FILTER_WORKERS}")

    match STRATEGY:
        case "default":
            FILTERED_TX = os.environ["FILTERED_TX"]
            UC2_TRANSACTIONS_TX = os.environ["UC2_TRANSACTIONS_TX"]
            UC5_TRANSACTIONS_TX = os.environ["UC5_TRANSACTIONS_TX"]
            routes = [
                (QueueRabbitMQ(MOM_HOST, FILTERED_TX), UC1Filter()),
                (QueueRabbitMQ(MOM_HOST, UC2_TRANSACTIONS_TX), UC2Filter()),
                (QueueRabbitMQ(MOM_HOST, UC5_TRANSACTIONS_TX), UC5Filter()),
            ]
        case "uc5_amount":
            UC5_AMOUNT_FILTERED_TX = os.environ["UC5_AMOUNT_FILTERED_TX"]
            routes = [
                (QueueRabbitMQ(MOM_HOST, UC5_AMOUNT_FILTERED_TX), UC5AmountFilter()),
            ]
        case _:
            raise ValueError(f"unknown filter strategy: {STRATEGY}")

    Filter(FILTER_ID, transactions_rx, ring_rx, ring_tx, routes).start()


if __name__ == "__main__":
    main()
