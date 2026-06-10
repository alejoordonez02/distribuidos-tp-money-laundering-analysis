from src import AggregateStrategy, GroupByStrategy, MergeStrategy

from .common_queues import CLIENT_ACCOUNTS, UC2_FILTERED_TRANSACTIONS, UC2_JOIN
from .container_type import ContainerType
from .gen_merge import gen_merge
from .gen_nodes import gen_nodes

MAX_AMOUNT_GROUPBYS = 2
MAX_AMOUNT_AGGREGATES = 1

BANK_NAMES_GROUPBYS = 2
BANK_NAMES_AGGREGATES = 1


def gen_uc2() -> str:
    compose = "\n# === uc2 ==="
    max_amounts_to_aggregate = "uc2_partial_max_amount"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC2_MAX_AMOUNT,
        npeers=MAX_AMOUNT_GROUPBYS,
        affinity_upstream=False,
        naffinity_downstream=MAX_AMOUNT_AGGREGATES,
        rx_name=UC2_FILTERED_TRANSACTIONS,
        tx_name=max_amounts_to_aggregate,
        checkpoint_every=5,
    )
    max_amounts_to_merge = "uc2_max_amounts_by_bank"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC2_MAX_AMOUNT,
        npeers=MAX_AMOUNT_AGGREGATES,
        affinity_upstream=True,
        naffinity_downstream=0,
        rx_name=max_amounts_to_aggregate,
        tx_name=max_amounts_to_merge,
        checkpoint_every=5,
    )

    bank_names_to_aggregate = "uc2_partial_bank_names"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC2_BANK_NAMES,
        npeers=BANK_NAMES_GROUPBYS,
        affinity_upstream=False,
        naffinity_downstream=BANK_NAMES_AGGREGATES,
        rx_name=CLIENT_ACCOUNTS,
        tx_name=bank_names_to_aggregate,
        checkpoint_every=5,
    )
    bank_names_to_merge = "uc2_bank_id_name_mappings"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC2_BANK_NAMES,
        npeers=BANK_NAMES_AGGREGATES,
        affinity_upstream=True,
        naffinity_downstream=0,
        rx_name=bank_names_to_aggregate,
        tx_name=bank_names_to_merge,
        checkpoint_every=5,
    )

    compose += gen_merge(
        strategy=MergeStrategy.UC2_MERGE,
        left_rx_name=max_amounts_to_merge,
        right_rx_name=bank_names_to_merge,
        tx_name=UC2_JOIN,
    )
    return compose
