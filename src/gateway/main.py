import logging
import os
from socket import AF_INET, SOCK_STREAM, socket

from common.comms.middleware import (
    QueueRabbitMQ,
)
from gateway import Gateway

GATEWAY_HOST = os.environ["GATEWAY_HOST"]
GATEWAY_PORT = os.environ["GATEWAY_PORT"]
MOM_HOST = os.environ["MOM_HOST"]
SERVER_QUEUE_RX = os.environ["SERVER_QUEUE_RX"]
TRANSACTIONS_TX = os.environ["TRANSACTIONS_TX"]
ACCOUNTS_TX = os.environ["ACCOUNTS_TX"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    listener = socket(AF_INET, SOCK_STREAM)
    addr = (GATEWAY_HOST, int(GATEWAY_PORT))
    server_rx = QueueRabbitMQ(MOM_HOST, SERVER_QUEUE_RX)

    def trans_tx_factory():
        return QueueRabbitMQ(MOM_HOST, TRANSACTIONS_TX)

    def accs_tx_factory():
        return QueueRabbitMQ(MOM_HOST, ACCOUNTS_TX)

    gateway = Gateway(listener, addr, server_rx, trans_tx_factory, accs_tx_factory)
    gateway.start()


if __name__ == "__main__":
    main()
