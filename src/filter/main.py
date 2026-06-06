import logging
import os
from typing import Callable

from filter2 import Filter
from filter_fns import FilterFn, UC3AvgFilter, UC4PathFilter, UC5Filter

from common.comms.eof_handler import make_stateless_eof_handler
from common.comms.middleware import ExchangeRabbitMQ, QueueRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
RX = os.environ["RX"]
STRATEGY = os.environ["STRATEGY"]
# This reading half varies depending on the controller instance that's being
# used, thus it does not need to be declared for each strategy.

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")


def make_default_filter() -> Filter:
    from filter_fns import (
        UC1Filter,
        UC2Filter,
        UC3FilterPeriodA,
        UC3FilterPeriodB,
        UC4Filter,
        UC5Filter,
    )

    UC1_TRANSACTIONS_TX = os.environ["UC1_TRANSACTIONS_TX"]
    UC2_TRANSACTIONS_TX = os.environ["UC2_TRANSACTIONS_TX"]
    UC3_PERIOD_A_TRANSACTIONS_TX = os.environ["UC3_PERIOD_A_TRANSACTIONS_TX"]
    UC3_PERIOD_B_TRANSACTIONS_TX = os.environ["UC3_PERIOD_B_TRANSACTIONS_TX"]
    UC4_TRANSACTIONS_TX = os.environ["UC4_TRANSACTIONS_TX"]
    UC4_DEGREE_TRANSACTIONS_TX = os.environ["UC4_DEGREE_TRANSACTIONS_TX"]
    UC5_TRANSACTIONS_TX = os.environ["UC5_TRANSACTIONS_TX"]

    transactions_rx = QueueRabbitMQ(MOM_HOST, RX)

    routes = [
        (QueueRabbitMQ(MOM_HOST, UC1_TRANSACTIONS_TX), UC1Filter()),
        (QueueRabbitMQ(MOM_HOST, UC2_TRANSACTIONS_TX), UC2Filter()),
        (
            QueueRabbitMQ(MOM_HOST, UC3_PERIOD_A_TRANSACTIONS_TX),
            UC3FilterPeriodA(),
        ),
        (
            QueueRabbitMQ(MOM_HOST, UC3_PERIOD_B_TRANSACTIONS_TX),
            UC3FilterPeriodB(),
        ),
        (QueueRabbitMQ(MOM_HOST, UC4_TRANSACTIONS_TX), UC4Filter()),
        (QueueRabbitMQ(MOM_HOST, UC4_DEGREE_TRANSACTIONS_TX), UC4Filter()),
        (QueueRabbitMQ(MOM_HOST, UC5_TRANSACTIONS_TX), UC5Filter()),
    ]

    # TODO: reescribir esto, las listas se
    #       declaran una vez mejor :)
    eof_handler = make_stateless_eof_handler(MOM_HOST, [tx for (tx, _) in routes])

    filter2 = Filter(transactions_rx, routes, eof_handler)

    return filter2


# fn: GroupByFn, idx: int, naffinities_downstream: int, mom_host: str, rx: str, tx: str
def make_filter(
    fn_factory: Callable[[], FilterFn],
    idx: int,
    affinity_upstream: bool,
    nnodes_downstream: int,
    mom_host: str,
    rx: str,
    tx: str,
) -> Filter:

    if affinity_upstream:
        external_rx = QueueRabbitMQ(MOM_HOST, rx)
    else:
        external_rx = ExchangeRabbitMQ(mom_host, rx, [f"{idx}"], f"{rx}{idx}")

    if nnodes_downstream == 0:
        external_txs = (QueueRabbitMQ(mom_host, queue_name=f"{tx}"),)
    elif nnodes_downstream == 1:
        external_txs = (QueueRabbitMQ(mom_host, queue_name=f"{tx}0"),)
    elif nnodes_downstream > 1:
        external_txs = [
            ExchangeRabbitMQ(mom_host, tx, routing_keys=[f"{n}"], queue_name=f"{tx}{n}")
            for n in range(nnodes_downstream)
        ]
        # TODO: medio q queda paja porq lo había
        #       hecho pensando más q nada para el
        #       default
        raise ValueError("downstream affinity not implemented for filter yet")

    else:
        raise ValueError("downstream nodes amount cannot be less than 0")

    # FIXME: esto lo estoy dejando porq estaba
    #        así, pero la verdad ya no me acuerdo
    #        si le pegábamos el eof a todos los
    #        de adelante... no debería tener
    #        mucho sentido
    eof_handler = make_stateless_eof_handler(MOM_HOST, (external_txs[0],))

    filter2 = Filter(
        external_rx, [(tx, fn_factory()) for tx in external_txs], eof_handler
    )

    return filter2


def main():
    logging.basicConfig(level=LOGGING_LEVEL)
    logging.getLogger("pika").setLevel(logging.WARNING)

    match STRATEGY:
        case "default":
            filter2 = make_default_filter()
            return filter2.start()

        case "uc3_avg":
            fn = UC3AvgFilter
        case "uc4_path":
            fn = UC4PathFilter
        case "uc5_amount":
            fn = UC5Filter
        case _:
            raise ValueError(f"unknown filter strategy: {STRATEGY}")

    TX = os.environ["TX"]

    IDX = int(os.getenv("IDX", 0))
    AFFINITY_UPSTREAM = bool(os.environ["AFFINITY_UPSTREAM"])
    NAFFINITY_DOWNSTREAM = int(os.environ["NAFFINITY_DOWNSTREAM"])

    filter2 = make_filter(
        fn, IDX, AFFINITY_UPSTREAM, NAFFINITY_DOWNSTREAM, MOM_HOST, RX, TX
    )

    logging.info(
        f"""
        starting filter: fn={type(fn)}, \
        idx={IDX}, affinity_upstream={AFFINITY_UPSTREAM}, \
        nnodes_downstream={NAFFINITY_DOWNSTREAM}, \
        mom_host={MOM_HOST}, rx={RX}, tx={TX}"\
        """
    )
    filter2.start()


if __name__ == "__main__":
    main()
