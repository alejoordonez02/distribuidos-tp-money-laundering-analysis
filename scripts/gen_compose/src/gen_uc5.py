from src import FilterStrategy, GroupByStrategy

from . import topology as topo
from .common_queues import UC5_JOIN, UC5_TRANSACTIONS
from .container_type import ContainerType
from .gen_nodes import gen_nodes


def gen_uc5() -> str:
    compose = "\n# === uc5 ==="
    queue0 = "uc5_converted_transactions"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC5_CONVERTER,
        npeers=topo.UC5_CONVERTERS,
        naffinity_downstream=topo.UC5_AMOUNT_FILTERS,
        rx_name=UC5_TRANSACTIONS,
        tx_name=queue0,
        checkpoint_every=5,
    )
    queue1 = "uc5_filtered_converted_transactions"
    compose += gen_nodes(
        type2=ContainerType.FILTER,
        strategy=FilterStrategy.UC5_AMOUNT,
        npeers=topo.UC5_AMOUNT_FILTERS,
        naffinity_downstream=topo.UC5_COUNT_GROUP_BYS,
        rx_name=queue0,
        tx_name=queue1,
        checkpoint_every=5,
    )
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC5_COUNT,
        npeers=topo.UC5_COUNT_GROUP_BYS,
        naffinity_downstream=0,
        rx_name=queue1,
        tx_name=UC5_JOIN,
        checkpoint_every=5,
    )
    return compose
