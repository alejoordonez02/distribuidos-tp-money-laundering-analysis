import logging
import os

from group_by_fns import (
    UC2BankNamesGroupByFn,
    UC2MaxAmountGroupByFn,
    UC3SumGroupByFn,
    UC4ComputeGraph,
    UC4CountPaths,
    UC5CountGroupByFn,
)

from common.comms.eof_handler import make_stateless_eof_handler
from common.comms.middleware import ExchangeRabbitMQ, QueueRabbitMQ
from group_by import GroupBy

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]
NPEERS_UPSTREAM = int(os.getenv("NPEERS_UPSTREAM", "1"))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def make_uc4_compute_graph():
    NNODES_DOWNSTREAM = int(os.environ["NNODES_DOWNSTREAM"])

    if NNODES_DOWNSTREAM <= 0:
        raise ValueError("downstream nodes amount cannot be less than 0")

    fn = UC4ComputeGraph()

    external_rx = QueueRabbitMQ(MOM_HOST, RX)
    external_txs = [
        ExchangeRabbitMQ(MOM_HOST, TX, routing_keys=[f"{n}"], queue_name=f"{TX}{n}")
        for n in range(NNODES_DOWNSTREAM)
    ]

    # TODO: quizás estaría bueno elegir
    #       random de manera dinámica?
    #       para no mandar siempre al
    #       mismo.
    eof_handler = make_stateless_eof_handler(MOM_HOST, (external_txs[0],))

    groupby = GroupBy(fn, external_rx, external_txs, eof_handler)
    groupby.start()


def make_uc4_count_paths():
    NNODES_DOWNSTREAM = int(os.environ["NNODES_DOWNSTREAM"])

    if NNODES_DOWNSTREAM <= 0:
        raise ValueError("downstream nodes amount cannot be less than 0")

    fn = UC4CountPaths()

    external_rx = QueueRabbitMQ(MOM_HOST, RX)
    external_txs = [
        ExchangeRabbitMQ(MOM_HOST, TX, routing_keys=[f"{n}"], queue_name=f"{TX}{n}")
        for n in range(NNODES_DOWNSTREAM)
    ]

    # TODO: quizás estaría bueno elegir
    #       random de manera dinámica?
    #       para no mandar siempre al
    #       mismo.
    eof_handler = make_stateless_eof_handler(MOM_HOST, (external_txs[0],))

    groupby = GroupBy(fn, external_rx, external_txs, eof_handler)
    groupby.start()


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
            return make_uc4_compute_graph()
        case "uc4_count_paths":
            return make_uc4_count_paths()
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
