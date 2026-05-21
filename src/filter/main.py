import logging
import os

from filter2 import Filter
from filter_fns import UC1Filter, UC2Filter, UC3FilterPeriodA, UC3FilterPeriodB

from common.comms.middleware import QueueRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
TRANSACTIONS_RX = os.environ["TRANSACTIONS_RX"]
FILTERED_TX = os.environ["FILTERED_TX"]
UC2_TRANSACTIONS_TX = os.environ["UC2_TRANSACTIONS_TX"]
UC3_PERIOD_A_TRANSACTIONS_TX = os.environ["UC3_PERIOD_A_TRANSACTIONS_TX"]
UC3_PERIOD_B_TRANSACTIONS_TX = os.environ["UC3_PERIOD_B_TRANSACTIONS_TX"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    transactions_rx = QueueRabbitMQ(MOM_HOST, TRANSACTIONS_RX)

    # Each route: (destination queue, filter function to apply before sending)
    routes = [
        (QueueRabbitMQ(MOM_HOST, FILTERED_TX), UC1Filter()),
        (QueueRabbitMQ(MOM_HOST, UC2_TRANSACTIONS_TX), UC2Filter()),
        (QueueRabbitMQ(MOM_HOST, UC3_PERIOD_A_TRANSACTIONS_TX), UC3FilterPeriodA()),
        (QueueRabbitMQ(MOM_HOST, UC3_PERIOD_B_TRANSACTIONS_TX), UC3FilterPeriodB())
    ]

    Filter(transactions_rx, routes).start()


if __name__ == "__main__":
    main()
