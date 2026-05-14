import logging
import os

from filter2 import Filter
from filter_fns import UC1Filter

from common.comms.middleware import QueueRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
TRANSACTIONS_RX = os.environ["TRANSACTIONS_RX"]
FILTERED_TX = os.environ["FILTERED_TX"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)

    transactions_rx = QueueRabbitMQ(MOM_HOST, TRANSACTIONS_RX)
    transactions_tx = QueueRabbitMQ(MOM_HOST, FILTERED_TX)

    filter_fn = UC1Filter()
    filter2 = Filter(transactions_rx, [(transactions_tx, filter_fn)])

    filter2.start()


if __name__ == "__main__":
    main()
