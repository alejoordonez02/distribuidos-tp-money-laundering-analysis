from src import AggregateStrategy, GroupByStrategy, MergeStrategy

from . import topology as topo
from .common_queues import CLIENT_ACCOUNTS, UC2_FILTERED_TRANSACTIONS, UC2_JOIN
from .container_type import ContainerType
from .gen_merge import gen_merge
from .gen_nodes import gen_nodes


def gen_uc2() -> str:
    compose = "\n# === uc2 ==="
    max_amounts_to_aggregate = "uc2_partial_max_amount"
    # stateless group_by (per-message fan-out by bank); affinity ring so each peer owns
    # its filtered-transactions shard (default filters shard by identity) and a crash
    # re-emit lands on the same peer -> dedup catches it, no aggregate count inflation.
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC2_MAX_AMOUNT,
        npeers=topo.UC2_MAX_AMOUNT_GROUP_BYS,
        naffinity_downstream=topo.UC2_MAX_AMOUNT_AGGREGATES,
        rx_name=UC2_FILTERED_TRANSACTIONS,
        tx_name=max_amounts_to_aggregate,
        checkpoint_every=5,
    )
    max_amounts_to_merge = "uc2_max_amounts_by_bank"
    # the merge is a broadcast-join: the max-by-bank state (left) is BROADCAST so every
    # merge peer holds it in full, while the bank-id->name mappings (right) are SHARDED
    # across the peers, so the peers' outputs partition the banks with no overlap.
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC2_MAX_AMOUNT,
        npeers=topo.UC2_MAX_AMOUNT_AGGREGATES,
        naffinity_downstream=topo.UC2_MERGES,
        broadcast_downstream=True,
        rx_name=max_amounts_to_aggregate,
        tx_name=max_amounts_to_merge,
        checkpoint_every=5,
    )

    bank_names_to_aggregate = "uc2_partial_bank_names"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC2_BANK_NAMES,
        npeers=topo.UC2_BANK_NAMES_GROUP_BYS,
        naffinity_downstream=topo.UC2_BANK_NAMES_AGGREGATES,
        rx_name=CLIENT_ACCOUNTS,
        tx_name=bank_names_to_aggregate,
        checkpoint_every=5,
    )
    bank_names_to_merge = "uc2_bank_id_name_mappings"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC2_BANK_NAMES,
        npeers=topo.UC2_BANK_NAMES_AGGREGATES,
        naffinity_downstream=topo.UC2_MERGES,
        rx_name=bank_names_to_aggregate,
        tx_name=bank_names_to_merge,
        checkpoint_every=5,
    )

    compose += gen_merge(
        strategy=MergeStrategy.UC2_MERGE,
        left_rx_name=max_amounts_to_merge,
        right_rx_name=bank_names_to_merge,
        tx_name=UC2_JOIN,
        checkpoint_every=5,
        npeers=topo.UC2_MERGES,
    )
    return compose
