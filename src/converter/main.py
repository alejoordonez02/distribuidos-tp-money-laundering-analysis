import logging
import os

from converter import Converter
from converter_fns import UC5USDConverterFn

from common.comms.middleware import QueueRabbitMQ
from common.conversion import FrankfurterConversionAPI

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case "uc5_usd":
            fn = UC5USDConverterFn(FrankfurterConversionAPI())
        case _:
            raise ValueError(f"unknown converter strategy: {STRATEGY}")

    Converter(QueueRabbitMQ(MOM_HOST, RX), fn, QueueRabbitMQ(MOM_HOST, TX)).start()


if __name__ == "__main__":
    main()
