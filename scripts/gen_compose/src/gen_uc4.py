from src import AggregateStrategy, GroupByStrategy, MergeStrategy

from .common_queues import UC4_DEGREE_TRANSACTIONS, UC4_JOIN, UC4_TRANSACTIONS
from .container_type import ContainerType
from .gen_merge import gen_merge
from .gen_nodes import gen_nodes

# The compute_graph group_bys are stateless (each builds a partial graph from ONE
# message's transactions and fans it out by node affinity; the aggregates do the
# cross-message merge). Scaled as affinity rings (RingGroupBy): the default filters
# shard the filtered/degree transactions across the N peers, so a crash re-emit lands
# on the same peer and its dedup catches the duplicate (no aggregate count inflation).
UC4_COMPUTE_GRAPHS = 3
UC4_DEGREE_COMPUTE_GRAPHS = 3

# The prune is a broadcast-join: the high-degree node set (left) is BROADCAST so
# every peer holds it in full, while the graph to prune (right) is SHARDED across the
# peers (each spills and prunes its shard against the broadcast high-degree set).
# get_result iterates the (sharded) spilled graph, so the peers' outputs partition the
# nodes with no overlap; the downstream count_paths aggregate dedups any re-emit.
UC4_PRUNES = 3


def gen_uc4() -> str:
    compose = "\n# === uc4 ==="
    queue0 = "uc4_graphs"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC4_COMPUTE_GRAPH,
        npeers=UC4_COMPUTE_GRAPHS,
        affinity_upstream=True,
        naffinity_downstream=3,
        rx_name=UC4_TRANSACTIONS,
        tx_name=queue0,
        checkpoint_every=5,
    )
    queue1 = "uc4_graphs_to_prune"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC4_AGGREGATE_GRAPHS,
        npeers=3,
        affinity_upstream=True,
        naffinity_downstream=UC4_PRUNES,
        rx_name=queue0,
        tx_name=queue1,
        checkpoint_every=5,
    )
    queue2 = "uc4_degree_graphs"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC4_DEGREE_COMPUTE_GRAPH,
        npeers=UC4_DEGREE_COMPUTE_GRAPHS,
        affinity_upstream=True,
        naffinity_downstream=3,
        rx_name=UC4_DEGREE_TRANSACTIONS,
        tx_name=queue2,
        checkpoint_every=5,
    )
    queue3 = "uc4_high_degree"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC4_DEGREE,
        npeers=3,
        affinity_upstream=True,
        naffinity_downstream=UC4_PRUNES,
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
        naffinity_downstream=5,
        npeers=UC4_PRUNES,
    )
    queue5 = "uc4_paths"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC4_COUNT_PATHS,
        npeers=5,
        affinity_upstream=True,
        naffinity_downstream=5,
        rx_name=queue4,
        tx_name=queue5,
        checkpoint_every=5,
    )
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC4_PATHS,
        npeers=5,
        affinity_upstream=True,
        naffinity_downstream=0,
        rx_name=queue5,
        tx_name=UC4_JOIN,
        checkpoint_every=5,
    )
    return compose
