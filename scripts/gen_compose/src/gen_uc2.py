from .common_queues import CLIENT_ACCOUNTS, UC2_FILTERED_TRANSACTIONS, UC2_JOIN
from .container_type import ContainerType
from .gen_merge import gen_merge
from .gen_nodes import gen_nodes


def gen_uc2() -> str:
    compose = "\n# === uc2 ===\n"
    queue0 = "uc2_partial_max_amount"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        name="uc2_max_amount_group_by",
        strategy="uc2_max_amount",
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=UC2_FILTERED_TRANSACTIONS,
        tx_name=queue0,
    )
    queue1 = "uc2_max_amounts_by_bank"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc2_max_amount_aggregate",
        strategy="uc2_max_amount",
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=queue0,
        tx_name=queue1,
    )
    queue2 = "uc2_partial_bank_names"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        name="uc2_bank_names_group_by",
        strategy="uc2_bank_names",
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=CLIENT_ACCOUNTS,
        tx_name=queue2,
    )
    queue3 = "uc2_bank_id_name_mappings"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc2_bank_names_aggregate",
        strategy="uc2_bank_names",
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=queue2,
        tx_name=queue3,
    )
    compose += gen_merge(
        name="uc2_merge",
        strategy="uc2_merge",
        left_rx_name=queue1,
        right_rx_name=queue3,
        tx_name=UC2_JOIN,
    )
    return compose
