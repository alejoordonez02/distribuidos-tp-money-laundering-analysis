from .common_queues import UC4_DEGREE_TRANSACTIONS, UC4_JOIN, UC4_TRANSACTIONS
from .container_type import ContainerType
from .gen_merge import gen_merge
from .gen_nodes import gen_nodes


def gen_uc4() -> str:
    compose = "\n# === uc4 ===\n"
    queue0 = "uc4_graphs"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        name="uc4_compute_graph_group_by",
        strategy="uc4_compute_graph",
        npeers=3,
        affinity_upstream=False,
        naffinity_downstream=3,
        rx_name=UC4_TRANSACTIONS,
        tx_name=queue0,
    )
    queue1 = "uc4_graphs_to_prune"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc4_aggregate_graphs",
        strategy="uc4_aggregate_graphs",
        npeers=3,
        affinity_upstream=True,
        naffinity_downstream=5,
        rx_name=queue0,
        tx_name=queue1,
    )
    queue2 = "uc4_degree_graphs"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        name="uc4_degree_compute_graph",
        strategy="uc4_compute_graph",
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=2,
        rx_name=UC4_DEGREE_TRANSACTIONS,
        tx_name=queue2,
    )
    queue3 = "uc4_high_degree"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc4_degree_aggregate",
        strategy="uc4_degree",
        npeers=2,
        affinity_upstream=True,
        naffinity_downstream=1,
        rx_name=queue2,
        tx_name=queue3,
    )
    queue4 = "uc4_pruned"
    compose += gen_merge(
        name="uc4_prune",
        strategy="uc4_prune",
        left_rx_name=queue3,
        right_rx_name=queue1,
        tx_name=queue4,
    )
    queue5 = "uc4_paths"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc4_count_paths",
        strategy="uc4_count_paths",
        npeers=5,
        affinity_upstream=False,
        naffinity_downstream=5,
        rx_name=queue4,
        tx_name=queue5,
    )
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc4_paths_aggregate",
        strategy="uc4_paths",
        npeers=5,
        affinity_upstream=True,
        naffinity_downstream=0,
        rx_name=queue5,
        tx_name=UC4_JOIN,
    )
    return compose
