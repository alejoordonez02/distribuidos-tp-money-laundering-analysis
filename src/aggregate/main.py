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
from strategies import AggregateStrategy

from aggregate import Aggregate
from common.comms.eof_handler import make_stateful_eof_handler
from common.comms.messages import EOF
from common.comms.middleware import make_rx_tx

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

IDX = int(os.getenv("IDX", 0))
AFFINITY_UPSTREAM = os.environ["AFFINITY_UPSTREAM"] == "1"
NAFFINITY_DOWNSTREAM = int(os.getenv("NAFFINITY_DOWNSTREAM", 0))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def make_aggregate(
    fn: AggregateFn,
    idx: int,
    affinity_upstream: bool,
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
) -> Aggregate:

    external_rx, external_txs = make_rx_tx(
        idx, rx_name, tx_name, mom_host, naffinities_downstream, affinity_upstream
    )

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
        case AggregateStrategy.UC2_MAX_AMOUNT:
            fn = UC2MaxAmountAggregateFn()
        case AggregateStrategy.UC2_BANK_NAMES:
            fn = UC2BankNamesAggregateFn()
        case AggregateStrategy.UC3_AVERAGE:
            fn = UC3AvgAggregateFn()
        case AggregateStrategy.UC4_COUNT_PATHS:
            fn = UC4CountPaths()
        case AggregateStrategy.UC4_AGGREGATE_GRAPHS:
            fn = UC4AggregateGraphs()
        case AggregateStrategy.UC4_PATHS:
            fn = UC4AggregatePaths()
        case AggregateStrategy.UC4_DEGREE:
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
