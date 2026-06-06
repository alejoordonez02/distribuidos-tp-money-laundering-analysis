from enum import StrEnum

CLIENT_TRANSACTIONS = "client_transactions"
CLIENT_ACCOUNTS = "client_accounts"

UC2_FILTERED_TRANSACTIONS = "uc2_filtered_transactions"
UC3_PERIOD_A_TRANSACTIONS = "uc3_transactons_period_A"
UC3_PERIOD_B_TRANSACTIONS = "uc3_transactons_period_B"
UC4_TRANSACTIONS = "uc4_filtered_transactions"
UC4_DEGREE_TRANSACTIONS = "uc4_degree_transactions"
UC5_TRANSACTIONS = "uc5_filtered_transactions"

UC1_JOIN = "uc1_join"
UC2_JOIN = "uc2_join"
UC3_JOIN = "uc3_join"
UC4_JOIN = "uc4_join"
UC5_JOIN = "uc5_join"


class ContainerType(StrEnum):
    FILTER = "filter"
    GROUP_BY = "group_by"
    AGGREGATE = "aggregate"
    CONVERTER = "converter"


# TODO: strategy debería ser el nombre pero
#       alta paja cambiarlo ahora en todos
#       lados
def gen_nodes(
    type2: ContainerType,
    name: str,
    strategy: str,
    npeers: int,
    affinity_upstream: bool,
    naffinity_downstream: int,
    rx_name: str,
    tx_name: str,
) -> str:
    compose = ""

    for idx in range(npeers):
        compose += f"""
  {name}_{idx}:
    build:
      context: ./src/
      dockerfile: {type2}/Dockerfile
    container_name: {name}_{idx}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - STRATEGY={strategy}
      - RX={rx_name}
      - TX={tx_name}
      - IDX={idx}
      - NPEERS={npeers}
      - RING_NAME={name}_ring
      - AFFINITY_UPSTREAM={1 if affinity_upstream else 0}
      - NAFFINITY_DOWNSTREAM={naffinity_downstream}
      - PYTHONHASHSEED=2026
      """

    return compose


def gen_default_filters(npeers: int) -> str:
    compose = ""

    for idx in range(npeers):
        compose += f"""
  default_filter_{idx}:
    build:
      context: ./src/
      dockerfile: filter/Dockerfile
    container_name: default_filter_{idx}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - RX=client_transactions
      - UC1_TRANSACTIONS_TX=uc1_join
      - UC2_TRANSACTIONS_TX=uc2_filtered_transactions
      - UC3_PERIOD_A_TRANSACTIONS_TX=uc3_transactons_period_A
      - UC3_PERIOD_B_TRANSACTIONS_TX=uc3_transactons_period_B
      - UC4_TRANSACTIONS_TX=uc4_filtered_transactions
      - UC4_DEGREE_TRANSACTIONS_TX=uc4_degree_transactions
      - UC5_TRANSACTIONS_TX=uc5_filtered_transactions
      - IDX={idx}
      - NPEERS={npeers}
      - RING_NAME=default_filter_ring
      - STRATEGY=default
      """

    return compose


def gen_merge(
    name: str,
    strategy: str,
    left_rx_name: str,
    right_rx_name: str,
    tx_name: str,
):
    return f"""
  {name}:
    build:
      context: ./src/
      dockerfile: merge/Dockerfile
    container_name: {name}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - LEFT_RX={left_rx_name}
      - RIGHT_RX={right_rx_name}
      - TX={tx_name}
      - STRATEGY={strategy}
      """


def gen_rabbitmq():
    return """
  # --- rabbitmq ---
  rabbitmq: # no le cambien nombre porq se usa en make down_server !!!
    build:
      context: ./src/rabbitmq
      dockerfile: Dockerfile
    container_name: rabbitmq
    environment:
      - RABBITMQ_LOG_LEVELS=error
      - RABBITMQ_CONFIG_FILE=rabbitmq.conf
    healthcheck:
      interval: 5s
      retries: 10
      start_period: 50s
      test: rabbitmq-diagnostics check_port_connectivity
      timeout: 3s
    ports:
      - 5672:5672
      - 15672:15672
"""


def gen_gateway():
    return """
  gateway:
    build:
      context: ./src/
      dockerfile: gateway/Dockerfile
    container_name: gateway
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - GATEWAY_HOST=gateway
      - GATEWAY_PORT=12345
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - SERVER_QUEUE_RX=responses
      - TRANSACTIONS_TX=client_transactions
      - ACCOUNTS_TX=client_accounts
      """


def gen_join():
    return """
  join:
    build:
      context: ./src/
      dockerfile: join/Dockerfile
    container_name: join
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - UC1_RX=uc1_join
      - UC2_RX=uc2_join
      - UC3_RX=uc3_join
      - UC4_RX=uc4_join
      - UC5_RX=uc5_join
      - RESPONSES_TX=responses
      """


def gen_uc1() -> str:
    compose = """
\n# === uc1 ===
# sólo usa default filtern"
    """
    return compose


def gen_uc2() -> str:
    compose = "\n# === uc2 ===\n"
    queue0 = "uc2_partial_max_amount"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        name="uc2_max_amount_group_by",
        strategy="uc2_max_amount",
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=UC2_FILTERED_TRANSACTIONS,
        tx_name=queue0,
    )
    queue1 = "uc2_max_amounts_by_bank"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc2_max_amount_aggregate",
        strategy="uc2_max_amount",
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=queue0,
        tx_name=queue1,
    )
    queue2 = "uc2_partial_bank_names"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        name="uc2_bank_names_group_by",
        strategy="uc2_bank_names",
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=CLIENT_ACCOUNTS,
        tx_name=queue2,
    )
    queue3 = "uc2_bank_id_name_mappings"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc2_bank_names_aggregate",
        strategy="uc2_bank_names",
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=queue2,
        tx_name=queue3,
    )
    compose += gen_merge(
        name="uc2_merge",
        strategy="uc2_merge",
        left_rx_name=queue1,
        right_rx_name=queue3,
        tx_name=UC2_JOIN,
    )
    return compose


def gen_uc3() -> str:
    compose = "\n# === uc3 ===\n"
    queue0 = "uc3_sum_by_format"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        name="uc3_group_by_format",
        strategy="uc3_sum",
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
        strategy="uc3_average",
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=queue0,
        tx_name=queue1,
    )
    queue2 = "uc3_average_merged"
    compose += gen_merge(
        name="uc3_merge",
        strategy="uc3_merge",
        left_rx_name=queue1,
        right_rx_name=UC3_PERIOD_B_TRANSACTIONS,
        tx_name=queue2,
    )
    compose += gen_nodes(
        type2=ContainerType.FILTER,
        name="uc3_average_filter",
        strategy="uc3_avg",
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,  # FIXME
        rx_name=queue2,
        tx_name=UC3_JOIN,
    )
    return compose


def gen_uc4() -> str:
    compose = "\n# === uc4 ===\n"
    queue0 = "uc4_graphs"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        name="uc4_compute_graph_group_by",
        strategy="uc4_compute_graph",
        npeers=3,
        affinity_upstream=False,
        naffinity_downstream=3,
        rx_name=UC4_TRANSACTIONS,
        tx_name=queue0,
    )
    queue1 = "uc4_graphs_to_prune"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc4_aggregate_graphs",
        strategy="uc4_aggregate_graphs",
        npeers=3,
        affinity_upstream=True,
        naffinity_downstream=5,
        rx_name=queue0,
        tx_name=queue1,
    )
    queue2 = "uc4_degree_graphs"
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        name="uc4_degree_compute_graph",
        strategy="uc4_compute_graph",
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=2,
        rx_name=UC4_DEGREE_TRANSACTIONS,
        tx_name=queue2,
    )
    queue3 = "uc4_high_degree"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc4_degree_aggregate",
        strategy="uc4_degree",
        npeers=2,
        affinity_upstream=True,
        naffinity_downstream=1,
        rx_name=queue2,
        tx_name=queue3,
    )
    queue4 = "uc4_pruned"
    compose += gen_merge(
        name="uc4_prune",
        strategy="uc4_prune",
        left_rx_name=queue3,
        right_rx_name=queue1,
        tx_name=queue4,
    )
    queue5 = "uc4_paths"
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc4_count_paths",
        strategy="uc4_count_paths",
        npeers=5,
        affinity_upstream=False,
        naffinity_downstream=5,
        rx_name=queue4,
        tx_name=queue5,
    )
    compose += gen_nodes(
        type2=ContainerType.AGGREGATE,
        name="uc4_paths_aggregate",
        strategy="uc4_paths",
        npeers=5,
        affinity_upstream=True,
        naffinity_downstream=0,
        rx_name=queue5,
        tx_name=UC4_JOIN,
    )
    return compose


def gen_uc5() -> str:
    compose = "\n# === uc5 ===\n"
    queue0 = "uc5_converted_transactions"
    # TODO: claramente esto tiene q ser otro
    #       groupby, o sea simplemente un
    #       stateless controller
    compose += gen_nodes(
        type2=ContainerType.CONVERTER,
        name="uc5_converter",
        strategy="",  # este no la usa
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=0,
        rx_name=UC5_TRANSACTIONS,
        tx_name=queue0,
    )
    queue1 = "uc5_filtered_converted_transactions"
    compose += gen_nodes(
        type2=ContainerType.FILTER,
        name="uc5_amount_filter",
        strategy="uc5_amount",  # este no la usa
        npeers=2,
        affinity_upstream=False,
        naffinity_downstream=0,
        rx_name=queue0,
        tx_name=queue1,
    )
    compose += gen_nodes(
        type2=ContainerType.GROUP_BY,
        name="uc5_count_group_by",
        strategy="uc5_count",  # este no la usa
        npeers=1,
        affinity_upstream=False,
        naffinity_downstream=0,
        rx_name=queue1,
        tx_name=UC5_JOIN,
    )
    return compose


NDEFAULT_FILTERS = 2


def main():
    compose = "services:\n"
    compose += gen_rabbitmq()
    compose += gen_gateway()
    compose += gen_default_filters(NDEFAULT_FILTERS)
    compose += gen_uc1()
    compose += gen_uc2()
    compose += gen_uc3()
    compose += gen_uc4()
    compose += gen_uc5()

    with open("test.yaml", "w") as f:
        f.write(compose)


if __name__ == "__main__":
    main()
