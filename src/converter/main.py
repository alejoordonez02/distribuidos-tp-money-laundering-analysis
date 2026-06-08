import logging
import os

from converter_fns import UC5USDConverterFn

from common.comms.middleware import QueueRabbitMQ
from common.conversion import FrankfurterConversionAPI
from converter import Converter

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    Converter(
        QueueRabbitMQ(MOM_HOST, RX),
        UC5USDConverterFn(FrankfurterConversionAPI()),
        QueueRabbitMQ(MOM_HOST, TX),
    ).start()


if __name__ == "__main__":
    main()
