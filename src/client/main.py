import logging
import os
import time
from socket import AF_INET, SOCK_STREAM, socket

from client import Client  # type: ignore
from common.comms.connection import Connection

TRANSACTIONS_PATH = os.environ["TRANSACTIONS_PATH"]
ACCOUNTS_PATH = os.environ["ACCOUNTS_PATH"]
RESPONSES_PATH = os.environ["RESPONSES_PATH"]
GATEWAY_HOST = os.environ["GATEWAY_HOST"]
GATEWAY_PORT = os.environ["GATEWAY_PORT"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")

GATEWAY_CONNECT_RETRIES = 30


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    conn = None
    for _ in range(GATEWAY_CONNECT_RETRIES):
        try:
            skt = socket(AF_INET, SOCK_STREAM)
            skt.connect((GATEWAY_HOST, int(GATEWAY_PORT)))
            conn = Connection(skt)
            break
        except OSError:
            time.sleep(1)
    if conn is None:
        raise ConnectionError("could not connect to gateway after retries")

    client = Client(
        conn,
        TRANSACTIONS_PATH,
        ACCOUNTS_PATH,
        RESPONSES_PATH,
    )
    client.start()


if __name__ == "__main__":
    main()
