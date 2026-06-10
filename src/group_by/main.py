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
from strategies import GroupByStrategy

from common.checkpoint import make_checkpointer
from common.comms.eof_handler import make_stateless_eof_handler
from common.comms.middleware import make_rx_tx
from group_by import GroupBy

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
TX = os.environ["TX"]
STRATEGY = os.environ["STRATEGY"]

IDX = int(os.getenv("IDX", 0))
AFFINITY_UPSTREAM = os.environ["AFFINITY_UPSTREAM"] == "1"
NAFFINITY_DOWNSTREAM = int(os.environ["NAFFINITY_DOWNSTREAM"])

STATE_DIR = os.getenv("STATE_DIR")
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", 5))

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "WARNING")


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
        idx,
        rx_name,
        tx_name,
        mom_host,
        naffinities_downstream,
        affinity_upstream,
        durable_rx=STATE_DIR is not None,
        rx_prefetch=CHECKPOINT_EVERY if STATE_DIR else 1,
    )

    eof_handler = make_stateless_eof_handler(MOM_HOST, (external_txs[0],))

    checkpointer = make_checkpointer(
        STATE_DIR,
        f"{STRATEGY}_{idx}",
        external_txs,
        CHECKPOINT_EVERY,
        extra_state={"eof": eof_handler},
    )

    groupby = GroupBy(fn, external_rx, external_txs, eof_handler, checkpointer)

    return groupby


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case GroupByStrategy.UC2_MAX_AMOUNT:
            fn = UC2MaxAmountGroupByFn()
        case GroupByStrategy.UC2_BANK_NAMES:
            fn = UC2BankNamesGroupByFn()
        case GroupByStrategy.UC3_SUM:
            fn = UC3SumGroupByFn()
        case (
            GroupByStrategy.UC4_COMPUTE_GRAPH | GroupByStrategy.UC4_DEGREE_COMPUTE_GRAPH
        ):
            fn = UC4ComputeGraph()
        case GroupByStrategy.UC5_COUNT:
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
