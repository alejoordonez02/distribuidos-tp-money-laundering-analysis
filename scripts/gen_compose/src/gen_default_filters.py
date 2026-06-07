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
