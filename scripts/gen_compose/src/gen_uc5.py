from src import FilterStrategy, GroupByStrategy

from .common_queues import UC5_JOIN, UC5_TRANSACTIONS
from .container_type import ContainerType
from .gen_nodes import gen_nodes

# The whole UC5 chain (convert -> amount filter -> count group_by) is stateless and
# per-message, scaled as affinity rings so each stage shards its output to the next
# and every node owns its input shard (crash-safe, re-emits dedup downstream). The
# default filters shard the filtered transactions across the N converters.
UC5_CONVERTERS = 3
UC5_AMOUNT_FILTERS = 3
UC5_COUNT_GROUPBYS = 3


def gen_uc5() -> str:
    compose = "\n# === uc5 ==="
    queue0 = "uc5_converted_transactions"
    compose += gen_nodes(
        type2=ContainerType.CONVERTER,
        strategy=GroupByStrategy.UC5_CONVERTER,  # este no la usa
        npeers=UC5_CONVERTERS,
        naffinity_downstream=UC5_AMOUNT_FILTERS,
        rx_name=UC5_TRANSACTIONS,
        tx_name=queue0,
        checkpoint_every=5,
    )
    queue1 = "uc5_filtered_converted_transactions"
    compose += gen_nodes(
        type2=ContainerType.FILTER,
        strategy=FilterStrategy.UC5_AMOUNT,
        npeers=UC5_AMOUNT_FILTERS,
        naffinity_downstream=UC5_COUNT_GROUPBYS,
        rx_name=queue0,
        tx_name=queue1,
        checkpoint_every=5,
    )
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC5_COUNT,
        npeers=UC5_COUNT_GROUPBYS,
        naffinity_downstream=0,
        rx_name=queue1,
        tx_name=UC5_JOIN,
        checkpoint_every=5,
    )
    return compose
