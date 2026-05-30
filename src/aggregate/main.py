import logging
import os
from queue import Queue

from aggregate_fns import (
    UC2BankNamesAggregateFn,
    UC2MaxAmountAggregateFn,
    UC3AvgAggregateFn,
    UC4AggregateGraphs,
    UC4AggregatePaths,
)

from aggregate import Aggregate
from common.comms.eof_handler.make_eof_handler import make_stateful_eof_handler
from common.comms.eof_handler.single_node_eof_handler import (
    StatefulSingleNodeEOFHandler,
)
from common.comms.messages.eof import EOF
from common.comms.middleware import QueueRabbitMQ

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
            fn = UC2MaxAmountAggregateFn()
        case "uc2_bank_names":
            fn = UC2BankNamesAggregateFn()
        case "uc3_average":
            fn = UC3AvgAggregateFn()
        case "uc4_count_paths":
            fn = UC4AggregateGraphs()
        case "uc4_paths":
            fn = UC4AggregatePaths()
        case _:
            raise ValueError(f"unknown aggregate strategy: {STRATEGY}")

    external_rx = QueueRabbitMQ(MOM_HOST, RX)
    external_tx = QueueRabbitMQ(MOM_HOST, TX)

    internal_eofs = Queue[EOF]()
    eof_handler = make_stateful_eof_handler(MOM_HOST, [external_tx], internal_eofs)
    # TODO: tmp
    if not isinstance(eof_handler, StatefulSingleNodeEOFHandler):
        raise ValueError("scalability is not implemented for aggregator yet")

    aggregate = Aggregate(
        external_rx, fn, external_tx, eof_handler, internal_eofs, NPEERS_UPSTREAM
    )
    aggregate.start()


if __name__ == "__main__":
    main()
