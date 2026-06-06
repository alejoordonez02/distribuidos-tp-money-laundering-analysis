import logging
import os
from queue import Queue

from aggregate_fns import (
    AggregateFn,
    UC2BankNamesAggregateFn,
    UC2MaxAmountAggregateFn,
    UC3AvgAggregateFn,
    UC4AggregateGraphs,
    UC4AggregatePaths,
    UC4CountPaths,
    UC4Degree,
)

from aggregate import Aggregate
from common.comms.eof_handler.make_eof_handler import make_stateful_eof_handler
from common.comms.messages.eof import EOF
from common.comms.middleware import QueueRabbitMQ
from common.comms.middleware.exchange_rabbitmq import ExchangeRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

IDX = int(os.getenv("IDX", 0))
AFFINITY_UPSTREAM = bool(os.environ["AFFINITY_UPSTREAM"])
# si a esto le pongo ntx_downstreams
# conserva mejor semántica
NAFFINITY_DOWNSTREAM = int(os.getenv("NAFFINITY_DOWNSTREAM", 0))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def make_aggregate(
    fn: AggregateFn,
    idx: int,
    affinity_upstream: bool,
    nnodes_downstream: int,
    mom_host: str,
    rx: str,
    tx: str,
) -> Aggregate:

    if affinity_upstream:
        external_rx = QueueRabbitMQ(MOM_HOST, rx)
    else:
        external_rx = ExchangeRabbitMQ(mom_host, rx, [f"{idx}"], f"{rx}{idx}")

    if nnodes_downstream == 0:
        external_txs = (QueueRabbitMQ(mom_host, queue_name=f"{tx}"),)
    elif nnodes_downstream == 1:
        external_txs = (QueueRabbitMQ(mom_host, queue_name=f"{tx}0"),)
    elif nnodes_downstream > 1:
        external_txs = [
            ExchangeRabbitMQ(mom_host, TX, routing_keys=[f"{n}"], queue_name=f"{tx}{n}")
            for n in range(nnodes_downstream)
        ]

    else:
        raise ValueError("downstream nodes amount cannot be less than 0")

    internal_eofs = Queue[EOF]()
    # TODO: tengo que cambiar el external_txs[0]
    #       porq va a traer problemas para fault
    #       tolerance
    eof_handler = make_stateful_eof_handler(MOM_HOST, (external_txs[0],), internal_eofs)

    aggregate = Aggregate(fn, external_rx, external_txs, eof_handler, internal_eofs)

    return aggregate


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
            fn = UC4CountPaths()
        case "uc4_aggregate_graphs":
            fn = UC4AggregateGraphs()
        case "uc4_paths":
            fn = UC4AggregatePaths()
        case "uc4_degree":
            fn = UC4Degree()
        case _:
            raise ValueError(f"unknown aggregate strategy: {STRATEGY}")

    aggregate = make_aggregate(
        fn, IDX, AFFINITY_UPSTREAM, NAFFINITY_DOWNSTREAM, MOM_HOST, RX, TX
    )

    logging.info(
        f"""
        starting aggregate: fn={type(fn)}, \
        idx={IDX}, nnodes_downstream={NAFFINITY_DOWNSTREAM}, \
        mom_host={MOM_HOST}, rx={RX}, tx={TX}"\
        """
    )
    aggregate.start()


if __name__ == "__main__":
    main()
