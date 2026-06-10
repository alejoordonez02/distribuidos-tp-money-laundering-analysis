from src import FilterStrategy, GroupByStrategy

from .common_queues import UC5_JOIN, UC5_TRANSACTIONS
from .container_type import ContainerType
from .gen_nodes import gen_nodes


def gen_uc5() -> str:
    compose = "\n# === uc5 ==="
    queue0 = "uc5_converted_transactions"
    # TODO: claramente esto tiene q ser otro
    #       groupby, o sea simplemente un
    #       stateless controller
    compose += gen_nodes(
        type2=ContainerType.CONVERTER,
        strategy=GroupByStrategy.UC5_CONVERTER,  # este no la usa
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=0,
        rx_name=UC5_TRANSACTIONS,
        tx_name=queue0,
        checkpoint_every=5,
    )
    queue1 = "uc5_filtered_converted_transactions"
    compose += gen_nodes(
        type2=ContainerType.FILTER,
        strategy=FilterStrategy.UC5_AMOUNT,
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=0,
        rx_name=queue0,
        tx_name=queue1,
        checkpoint_every=5,
    )
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        strategy=GroupByStrategy.UC5_COUNT,
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,
        rx_name=queue1,
        tx_name=UC5_JOIN,
        checkpoint_every=5,
    )
    return compose
