import logging
import os

from filter2 import Filter
from filter_fns import UC1Filter, UC2Filter, UC3FilterPeriodA, UC3FilterPeriodB, UC3AvgFilter, UC5AmountFilter, UC5Filter

from common.comms.middleware import QueueRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
TRANSACTIONS_RX = os.environ["TRANSACTIONS_RX"]
STRATEGY = os.getenv("STRATEGY", "default")

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    transactions_rx = QueueRabbitMQ(MOM_HOST, TRANSACTIONS_RX)

    match STRATEGY:
        case "default":
            FILTERED_TX = os.environ["FILTERED_TX"]
            UC2_TRANSACTIONS_TX = os.environ["UC2_TRANSACTIONS_TX"]
            UC3_PERIOD_A_TRANSACTIONS_TX = os.environ["UC3_PERIOD_A_TRANSACTIONS_TX"]
            UC3_PERIOD_B_TRANSACTIONS_TX = os.environ["UC3_PERIOD_B_TRANSACTIONS_TX"]
            UC5_TRANSACTIONS_TX = os.environ["UC5_TRANSACTIONS_TX"]
            routes = [
                (QueueRabbitMQ(MOM_HOST, FILTERED_TX), UC1Filter()),
                (QueueRabbitMQ(MOM_HOST, UC2_TRANSACTIONS_TX), UC2Filter()),
                (QueueRabbitMQ(MOM_HOST, UC3_PERIOD_A_TRANSACTIONS_TX), UC3FilterPeriodA()),
                (QueueRabbitMQ(MOM_HOST, UC3_PERIOD_B_TRANSACTIONS_TX), UC3FilterPeriodB()),
                (QueueRabbitMQ(MOM_HOST, UC5_TRANSACTIONS_TX), UC5Filter())
            ]
        case "uc3_avg":
            UC3_FILTERED_TX = os.environ["UC3_FILTERED_TX"] 
            routes = [
                (QueueRabbitMQ(MOM_HOST, UC3_FILTERED_TX), UC3AvgFilter())
            ]
        case "uc5_amount":
            UC5_AMOUNT_FILTERED_TX = os.environ["UC5_AMOUNT_FILTERED_TX"]
            routes = [
                (QueueRabbitMQ(MOM_HOST, UC5_AMOUNT_FILTERED_TX), UC5AmountFilter()),
            ]
        case _:
            raise ValueError(f"unknown filter strategy: {STRATEGY}")
    Filter(transactions_rx, routes).start()


if __name__ == "__main__":
    main()
