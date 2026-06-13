from src import AggregateStrategy, GroupByStrategy, MergeStrategy

from . import topology as topo
from .common_queues import UC4_DEGREE_TRANSACTIONS, UC4_JOIN, UC4_TRANSACTIONS
from .container_type import ContainerType
from .gen_merge import gen_merge
from .gen_nodes import gen_nodes


def gen_uc4() -> str:
    compose = "\n# === uc4 ==="
    queue0 = "uc4_graphs"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC4_COMPUTE_GRAPH,
        npeers=topo.UC4_COMPUTE_GRAPHS,
        naffinity_downstream=topo.UC4_AGGREGATE_GRAPHS,
        rx_name=UC4_TRANSACTIONS,
        tx_name=queue0,
        checkpoint_every=5,
    )
    queue1 = "uc4_graphs_to_prune"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC4_AGGREGATE_GRAPHS,
        npeers=topo.UC4_AGGREGATE_GRAPHS,
        naffinity_downstream=topo.UC4_PRUNES,
        rx_name=queue0,
        tx_name=queue1,
        checkpoint_every=5,
    )
    queue2 = "uc4_degree_graphs"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC4_DEGREE_COMPUTE_GRAPH,
        npeers=topo.UC4_DEGREE_COMPUTE_GRAPHS,
        naffinity_downstream=topo.UC4_DEGREE_AGGREGATES,
        rx_name=UC4_DEGREE_TRANSACTIONS,
        tx_name=queue2,
        checkpoint_every=5,
    )
    queue3 = "uc4_high_degree"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC4_DEGREE,
        npeers=topo.UC4_DEGREE_AGGREGATES,
        naffinity_downstream=topo.UC4_PRUNES,
        broadcast_downstream=True,
        rx_name=queue2,
        tx_name=queue3,
        checkpoint_every=5,
    )
    queue4 = "uc4_pruned"
    compose += gen_merge(
        strategy=MergeStrategy.UC4_PRUNE,
        left_rx_name=queue3,
        right_rx_name=queue1,
        tx_name=queue4,
        checkpoint_every=5,
        naffinity_downstream=topo.UC4_COUNT_PATHS,
        npeers=topo.UC4_PRUNES,
    )
    queue5 = "uc4_paths"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC4_COUNT_PATHS,
        npeers=topo.UC4_COUNT_PATHS,
        naffinity_downstream=topo.UC4_PATHS_AGGREGATES,
        rx_name=queue4,
        tx_name=queue5,
        checkpoint_every=5,
    )
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC4_PATHS,
        npeers=topo.UC4_PATHS_AGGREGATES,
        naffinity_downstream=0,
        rx_name=queue5,
        tx_name=UC4_JOIN,
        checkpoint_every=5,
    )
    return compose
