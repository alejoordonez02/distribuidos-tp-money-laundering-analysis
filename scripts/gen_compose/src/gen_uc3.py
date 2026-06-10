from src import AggregateStrategy, FilterStrategy, GroupByStrategy, MergeStrategy

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
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC3_SUM,
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=0,
        rx_name=UC3_PERIOD_A_TRANSACTIONS,
        tx_name=queue0,
        checkpoint_every=5,
    )
    queue1 = "uc3_avg"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        strategy=AggregateStrategy.UC3_AVERAGE,
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,
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
    )
    compose += gen_nodes(
        type2=ContainerType.FILTER,
        strategy=FilterStrategy.UC3_AVG,
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,
        rx_name=queue2,
        tx_name=UC3_JOIN,
        checkpoint_every=5,
    )
    return compose
