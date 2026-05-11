import os
from socket import AF_INET, SOCK_STREAM, socket

from common.comms.middleware import (
    QueueRabbitMQ,
)

from .gateway import Gateway

GATEWAY_HOST = os.environ["GATEWAY_HOST"]
GATEWAY_PORT = os.environ["GATEWAY_PORT"]
MOM_HOST = os.environ["MOM_HOST"]
SERVER_QUEUE_RX = os.environ["SERVER_QUEUE_RX"]
TRANSACTIONS_TX = os.environ["TRANSACTIONS_TX"]
ACCOUNTS_TX = os.environ["ACCOUNTS_TX"]


def main():
    listener = socket(AF_INET, SOCK_STREAM)
    addr = (GATEWAY_HOST, int(GATEWAY_PORT))
    server_rx = QueueRabbitMQ(MOM_HOST, SERVER_QUEUE_RX)
    transactions_tx = QueueRabbitMQ(MOM_HOST, TRANSACTIONS_TX)
    accounts_tx = QueueRabbitMQ(MOM_HOST, ACCOUNTS_TX)

    gateway = Gateway(listener, addr, server_rx, transactions_tx, accounts_tx)
    gateway.start()


if __name__ == "__main__":
    main()
