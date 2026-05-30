import logging
import os

from group_by_fns import (
    UC2BankNamesGroupByFn,
    UC2MaxAmountGroupByFn,
    UC3SumGroupByFn,
    UC4ComputeGraph,
    UC5CountGroupByFn,
)

from common.comms.eof_handler import make_stateless_eof_handler
from common.comms.middleware import QueueRabbitMQ
from group_by import GroupBy

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]
NPEERS_UPSTREAM = int(os.getenv("NPEERS_UPSTREAM", "1"))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case "uc2_max_amount":
            fn = UC2MaxAmountGroupByFn()
        case "uc2_bank_names":
            fn = UC2BankNamesGroupByFn()
        case "uc3_sum":
            fn = UC3SumGroupByFn()
        case "uc4_compute_graph":
            fn = UC4ComputeGraph()
        case "uc5_count":
            fn = UC5CountGroupByFn()
        case _:
            raise ValueError(f"unknown group_by strategy: {STRATEGY}")

    external_rx = QueueRabbitMQ(MOM_HOST, RX)
    external_txs = (QueueRabbitMQ(MOM_HOST, TX),)
    eof_handler = make_stateless_eof_handler(MOM_HOST, external_txs)

    groupby = GroupBy(fn, external_rx, external_txs, eof_handler)
    groupby.start()


if __name__ == "__main__":
    main()
