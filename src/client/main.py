import os
from socket import AF_INET, SOCK_STREAM, socket

from client import Client
from common.comms.connection import Connection

TRANSACTIONS_PATH = os.environ["TRANSACTIONS_PATH"]
ACCOUNTS_PATH = os.environ["ACCOUNTS_PATH"]
RESPONSES_PATH = os.environ["RESPONSES_PATH"]
GATEWAY_HOST = os.environ["GATEWAY_HOST"]
GATEWAY_PORT = os.environ["GATEWAY_PORT"]


def main():
    skt = socket(AF_INET, SOCK_STREAM)
    skt.connect((GATEWAY_HOST, int(GATEWAY_PORT)))
    conn = Connection(skt)

    client = Client(conn, TRANSACTIONS_PATH, ACCOUNTS_PATH, RESPONSES_PATH)
    client.start()


if __name__ == "__main__":
    main()
