from src import AggregateStrategy, FilterStrategy, GroupByStrategy, MergeStrategy

from . import topology as topo
from .common_queues import (
    UC3_JOIN,
    UC3_PERIOD_A_TRANSACTIONS,
    UC3_PERIOD_B_TRANSACTIONS,
)
from .container_type import ContainerType
from .gen_merge import gen_merge
from .gen_nodes import gen_nodes


def gen_uc3() -> str:
    compose = "\n# === uc3 ==="
    queue0 = "uc3_sum_by_format"
    # stateless group_by (per-message fan-out by format); the default filters shard
    # period A across the peers, so a crash re-emit lands on the same peer and its
    # dedup catches it — no double-count inflating the aggregate's expected_count.
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC3_SUM,
        npeers=topo.UC3_GROUP_BYS,
        naffinity_downstream=topo.UC3_AGGREGATES,
        rx_name=UC3_PERIOD_A_TRANSACTIONS,
        tx_name=queue0,
        checkpoint_every=5,
    )
    queue1 = "uc3_avg"
    # reducer partitioned by payment_format: each aggregate owns a disjoint set of
    # formats and computes a correct average; the averages are BROADCAST to the merges.
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC3_AVERAGE,
        npeers=topo.UC3_AGGREGATES,
        naffinity_downstream=topo.UC3_MERGES,
        broadcast_downstream=True,
        rx_name=queue0,
        tx_name=queue1,
        checkpoint_every=5,
    )
    queue2 = "uc3_average_merged"
    # the real bottleneck: spills ALL of period B and streams it back against the
    # averages. Broadcast-join: averages (left) broadcast, period B (right) sharded by
    # the default filters, a barrier consolidates the per-peer outputs into one EOF.
    compose += gen_merge(
        strategy=MergeStrategy.UC3_MERGE,
        left_rx_name=queue1,
        right_rx_name=UC3_PERIOD_B_TRANSACTIONS,
        tx_name=queue2,
        checkpoint_every=5,
        npeers=topo.UC3_MERGES,
        naffinity_downstream=topo.UC3_FILTERS,
    )
    compose += gen_nodes(
        type2=ContainerType.FILTER,
        strategy=FilterStrategy.UC3_AVG,
        npeers=topo.UC3_FILTERS,
        naffinity_downstream=0,
        rx_name=queue2,
        tx_name=UC3_JOIN,
        checkpoint_every=5,
    )
    return compose
