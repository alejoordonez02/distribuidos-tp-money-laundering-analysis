import logging
import os

from join_fns import UC1Join, UC2Join, UC5Join

from common.comms.middleware import QueueRabbitMQ
from join import Join

MOM_HOST = os.environ["MOM_HOST"]
UC1_RX = os.environ["UC1_RX"]
UC2_RX = os.environ["UC2_RX"]
UC3_RX = os.environ["UC3_RX"]
UC5_RX = os.environ["UC5_RX"]
RESPONSES_TX = os.environ["RESPONSES_TX"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    partial_res_handlers = [
        (QueueRabbitMQ(MOM_HOST, UC1_RX), UC1Join()),
        (QueueRabbitMQ(MOM_HOST, UC2_RX), UC2Join()),
        # TODO: (QueueRabbitMQ(MOM_HOST, UC3_RX), UC3Join()),
        (QueueRabbitMQ(MOM_HOST, UC5_RX), UC5Join()),
    ]
    responses_tx = QueueRabbitMQ(MOM_HOST, RESPONSES_TX)

    join = Join(partial_res_handlers, responses_tx)
    join.start()


if __name__ == "__main__":
    main()
