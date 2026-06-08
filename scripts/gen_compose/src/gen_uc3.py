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
        name="uc3_group_by_format",
        strategy=GroupByStrategy.UC3_SUM,
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=UC3_PERIOD_A_TRANSACTIONS,
        tx_name=queue0,
    )
    queue1 = "uc3_avg"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc3_average_aggregate",
        strategy=AggregateStrategy.UC3_AVERAGE,
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=queue0,
        tx_name=queue1,
    )
    queue2 = "uc3_average_merged"
    compose += gen_merge(
        name="uc3_merge",
        strategy=MergeStrategy.UC3_MERGE,
        left_rx_name=queue1,
        right_rx_name=UC3_PERIOD_B_TRANSACTIONS,
        tx_name=queue2,
    )
    compose += gen_nodes(
        type2=ContainerType.FILTER,
        name="uc3_average_filter",
        strategy=FilterStrategy.UC3_AVG,
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=queue2,
        tx_name=UC3_JOIN,
    )
    return compose
