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


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    # Retry the connect: `depends_on` only guarantees the gateway container has
    # started, not that it is already listening (it waits for RabbitMQ first), so
    # the client can race ahead and get ECONNREFUSED.
    conn = None
    for _ in range(30):
        try:
            skt = socket(AF_INET, SOCK_STREAM)
            skt.connect((GATEWAY_HOST, int(GATEWAY_PORT)))
            conn = Connection(skt)
            break
        except OSError:
            time.sleep(1)
    if conn is None:
        raise ConnectionError("could not connect to gateway after retries")

    # The client gets its id from the gateway during the handshake (HelloAck),
    # then stamps it on every message so the gateway can forward batches raw.
    client = Client(
        conn,
        TRANSACTIONS_PATH,
        ACCOUNTS_PATH,
        RESPONSES_PATH,
    )
    client.start()


if __name__ == "__main__":
    main()
