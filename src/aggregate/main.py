import logging
import os
from queue import Queue

from aggregate_fns import (
    UC2BankNamesAggregateFn,
    UC2MaxAmountAggregateFn,
    UC3AvgAggregateFn,
    UC4AggregateGraphs,
    UC4AggregatePaths,
    UC4CountPaths,
)

from aggregate import Aggregate
from common.comms.eof_handler.make_eof_handler import make_stateful_eof_handler
from common.comms.eof_handler.single_node_eof_handler import (
    StatefulSingleNodeEOFHandler,
)
from common.comms.messages.eof import EOF
from common.comms.middleware import QueueRabbitMQ
from common.comms.middleware.exchange_rabbitmq import ExchangeRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def make_uc4_aggregate_graphs():
    IDX = int(os.environ["IDX"])
    NNODES_DOWNSTREAM = int(os.environ["NNODES_DOWNSTREAM"])

    fn = UC4AggregateGraphs()

    external_rx = ExchangeRabbitMQ(MOM_HOST, RX, [f"{IDX}"], f"{RX}{IDX}")
    external_txs = [
        ExchangeRabbitMQ(MOM_HOST, TX, routing_keys=[f"{n}"], queue_name=f"{TX}{n}")
        for n in range(NNODES_DOWNSTREAM)
    ]

    internal_eofs = Queue[EOF]()
    eof_handler = make_stateful_eof_handler(MOM_HOST, (external_txs[0],), internal_eofs)

    aggregate = Aggregate(fn, external_rx, external_txs, eof_handler, internal_eofs)
    aggregate.start()


def make_uc4_count_paths():
    IDX = int(os.environ["IDX"])
    NNODES_DOWNSTREAM = int(os.environ["NNODES_DOWNSTREAM"])

    fn = UC4CountPaths()

    external_rx = ExchangeRabbitMQ(MOM_HOST, RX, [f"{IDX}"], f"{RX}{IDX}")
    external_txs = [
        ExchangeRabbitMQ(MOM_HOST, TX, routing_keys=[f"{n}"], queue_name=f"{TX}{n}")
        for n in range(NNODES_DOWNSTREAM)
    ]

    internal_eofs = Queue[EOF]()
    eof_handler = make_stateful_eof_handler(MOM_HOST, (external_txs[0],), internal_eofs)

    aggregate = Aggregate(fn, external_rx, external_txs, eof_handler, internal_eofs)
    aggregate.start()


# TODO: deduplicar este código porfa
def make_uc4_aggregate_paths():
    IDX = int(os.environ["IDX"])

    fn = UC4AggregatePaths()

    external_rx = ExchangeRabbitMQ(MOM_HOST, RX, [f"{IDX}"], f"{RX}{IDX}")
    external_txs = (QueueRabbitMQ(MOM_HOST, TX),)

    internal_eofs = Queue[EOF]()
    eof_handler = make_stateful_eof_handler(MOM_HOST, (external_txs[0],), internal_eofs)

    aggregate = Aggregate(fn, external_rx, external_txs, eof_handler, internal_eofs)
    aggregate.start()


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case "uc2_max_amount":
            fn = UC2MaxAmountAggregateFn()
        case "uc2_bank_names":
            fn = UC2BankNamesAggregateFn()
        case "uc3_average":
            fn = UC3AvgAggregateFn()
        case "uc4_count_paths":
            return make_uc4_count_paths()
        case "uc4_aggregate_graphs":
            return make_uc4_aggregate_graphs()
        case "uc4_paths":
            return make_uc4_aggregate_paths()
        case _:
            raise ValueError(f"unknown aggregate strategy: {STRATEGY}")

    external_rx = QueueRabbitMQ(MOM_HOST, RX)
    external_txs = (QueueRabbitMQ(MOM_HOST, TX),)

    internal_eofs = Queue[EOF]()
    eof_handler = make_stateful_eof_handler(MOM_HOST, external_txs, internal_eofs)
    # TODO: tmp
    if not isinstance(eof_handler, StatefulSingleNodeEOFHandler):
        raise ValueError("scalability is not implemented for aggregator yet")

    aggregate = Aggregate(fn, external_rx, external_txs, eof_handler, internal_eofs)
    aggregate.start()


if __name__ == "__main__":
    main()
