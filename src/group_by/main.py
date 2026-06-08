import logging
import os

from group_by_fns import (
    GroupByFn,
    UC2BankNamesGroupByFn,
    UC2MaxAmountGroupByFn,
    UC3SumGroupByFn,
    UC4ComputeGraph,
    UC5CountGroupByFn,
)

from common.comms.eof_handler import make_stateless_eof_handler
from common.comms.middleware import ExchangeRabbitMQ, QueueRabbitMQ, make_rx_tx
from group_by import GroupBy

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

IDX = int(os.getenv("IDX", 0))
AFFINITY_UPSTREAM = os.environ["AFFINITY_UPSTREAM"] == "1"
NAFFINITY_DOWNSTREAM = int(os.environ["NAFFINITY_DOWNSTREAM"])

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def make_groupby(
    fn: GroupByFn,
    idx: int,
    affinity_upstream: bool,
    naffinities_downstream: int,
    mom_host: str,
    rx_name: str,
    tx_name: str,
) -> GroupBy:

    external_rx, external_txs = make_rx_tx(
        idx, rx_name, tx_name, mom_host, naffinities_downstream, affinity_upstream
    )

    # TODO: tengo que cambiar el external_txs[0]
    #       porq va a traer problemas para fault
    #       tolerance
    eof_handler = make_stateless_eof_handler(MOM_HOST, (external_txs[0],))

    groupby = GroupBy(fn, external_rx, external_txs, eof_handler)

    return groupby


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

    groupby = make_groupby(
        fn, IDX, AFFINITY_UPSTREAM, NAFFINITY_DOWNSTREAM, MOM_HOST, RX, TX
    )

    logging.info(
        f"""
        starting groupby: fn={type(fn)}, \
        idx={IDX}, nnodes_downstream={NAFFINITY_DOWNSTREAM}, \
        mom_host={MOM_HOST}, rx={RX}, tx={TX}"\
        """
    )
    groupby.start()


if __name__ == "__main__":
    main()
