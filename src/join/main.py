import logging
import os

from join_fns import UC1Join, UC2Join

from common.comms.middleware import QueueRabbitMQ
from join import Join

MOM_HOST = os.environ["MOM_HOST"]
CLIENT_RESPONSES_RX = os.environ["CLIENT_RESPONSES_RX"]
CLIENT_RESPONSES_TX = os.environ["CLIENT_RESPONSES_TX"]
STRATEGY = os.getenv("STRATEGY", "uc1")

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case "uc1":
            join_fn = UC1Join()
        case "uc2":
            join_fn = UC2Join()
        case _:
            raise ValueError(f"unknown join strategy: {STRATEGY}")

    partial_res_handlers = [(QueueRabbitMQ(MOM_HOST, CLIENT_RESPONSES_RX), join_fn)]
    responses_tx = QueueRabbitMQ(MOM_HOST, CLIENT_RESPONSES_TX)

    # NOTE: no sé por qué me tira error el linter acá...
    join = Join(partial_res_handlers, responses_tx)  # type: ignore[reportArgumentType]
    join.start()


if __name__ == "__main__":
    main()
