from src import AggregateStrategy, FilterStrategy, GroupByStrategy, MergeStrategy

from .common_queues import (
    UC3_JOIN,
    UC3_PERIOD_A_TRANSACTIONS,
    UC3_PERIOD_B_TRANSACTIONS,
)
from .container_type import ContainerType
from .gen_merge import gen_merge
from .gen_nodes import gen_nodes


# UC3_AVERAGE is a reducer partitioned by payment_format: the group_by routes its
# partial sums by hash(format) (affinity downstream) so each aggregate owns a
# disjoint set of formats and computes a correct average. Capped at the format
# cardinality (~7 payment formats); 3 maximizes that without idle peers.
UC3_AGGREGATES = 3

# The UC3_SUM group_by is stateless (per-message fan-out by format); scaled as an
# affinity ring (RingGroupBy) so each peer owns its period-A input shard and recovers
# crash-safely. The default filters shard period A across the N group_bys (by message
# identity), so a crash re-emit lands on the same peer and its dedup catches it — no
# double-count inflating the downstream aggregate's expected_count.
UC3_GROUP_BYS = 3

# The merge is the real bottleneck: it spills ALL of period B to disk and streams it
# back to join against the averages. It is scaled as a broadcast-join: the small
# averages (left) are BROADCAST so every merge peer holds them in full, while period
# B (right) is SHARDED across the peers (by the default filters, opt-in via
# UC3_PERIOD_B_SHARDS) so each peer spills/streams only its 1/N. A ring barrier
# consolidates the per-peer outputs into one downstream EOF, so the filter sees a
# single consolidated EOF exactly as from a single merge.
UC3_MERGES = 3

# The UC3_AVG filter is stateless; scaled as an affinity ring (RingFilter) so each
# peer owns its input shard and recovers crash-safely (vs the competing working-queue
# path).
UC3_FILTERS = 3


def gen_uc3() -> str:
    compose = "\n# === uc3 ==="
    queue0 = "uc3_sum_by_format"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC3_SUM,
        npeers=UC3_GROUP_BYS,
        affinity_upstream=True,
        naffinity_downstream=UC3_AGGREGATES,
        rx_name=UC3_PERIOD_A_TRANSACTIONS,
        tx_name=queue0,
        checkpoint_every=5,
    )
    queue1 = "uc3_avg"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC3_AVERAGE,
        npeers=UC3_AGGREGATES,
        affinity_upstream=True,
        naffinity_downstream=UC3_MERGES,
        broadcast_downstream=True,
        rx_name=queue0,
        tx_name=queue1,
        checkpoint_every=5,
    )
    queue2 = "uc3_average_merged"
    compose += gen_merge(
        strategy=MergeStrategy.UC3_MERGE,
        left_rx_name=queue1,
        right_rx_name=UC3_PERIOD_B_TRANSACTIONS,
        tx_name=queue2,
        checkpoint_every=5,
        npeers=UC3_MERGES,
        naffinity_downstream=UC3_FILTERS,
    )
    compose += gen_nodes(
        type2=ContainerType.FILTER,
        strategy=FilterStrategy.UC3_AVG,
        npeers=UC3_FILTERS,
        affinity_upstream=True,
        naffinity_downstream=0,
        rx_name=queue2,
        tx_name=UC3_JOIN,
        checkpoint_every=5,
    )
    return compose
