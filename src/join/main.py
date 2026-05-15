import logging
import os

from common.comms.middleware import QueueRabbitMQ
from join import Join

MOM_HOST = os.environ["MOM_HOST"]
CLIENT_RESPONSES_RX = os.environ["CLIENT_RESPONSES_RX"]
CLIENT_RESPONSES_TX = os.environ["CLIENT_RESPONSES_TX"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    client_responses_rx = QueueRabbitMQ(MOM_HOST, CLIENT_RESPONSES_RX)
    client_responses_tx = QueueRabbitMQ(MOM_HOST, CLIENT_RESPONSES_TX)

    join = Join(client_responses_rx, client_responses_tx)
    join.start()


if __name__ == "__main__":
    main()
