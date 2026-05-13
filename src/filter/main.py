import os

from filter2 import Filter
from filter_fns import Promiscuous

from common.comms.middleware import ExchangeRabbitMQ, QueueRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
TRANSACTIONS_RX = os.environ["TRANSACTIONS_RX"]
FILTERED_TX = os.environ["FILTERED_TX"]


def main():
    transactions_rx = QueueRabbitMQ(MOM_HOST, TRANSACTIONS_RX)
    transactions_tx = ExchangeRabbitMQ(MOM_HOST, FILTERED_TX, [""])

    filter_fn = Promiscuous()
    filter2 = Filter(transactions_rx, [(transactions_tx, filter_fn)])

    filter2.start()


if __name__ == "__main__":
    main()
