from .gen_nodes import CHECKPOINT_EVERY
from .gen_uc2 import MAX_AMOUNT_GROUPBYS
from .gen_uc3 import UC3_GROUP_BYS, UC3_MERGES
from .gen_uc4 import UC4_COMPUTE_GRAPHS, UC4_DEGREE_COMPUTE_GRAPHS
from .gen_uc5 import UC5_CONVERTERS

DEFAULT_FILTERS = 3


def gen_default_filters() -> str:
    compose = "\n# === default filters ==="

    for idx in range(DEFAULT_FILTERS):
        compose += f"""\n
  default_filter_{idx}:
    build:
      context: ./src/
      dockerfile: filter/Dockerfile
    container_name: default_filter_{idx}
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - RX=client_transactions
      - UC1_TRANSACTIONS_TX=uc1_join
      - UC2_TRANSACTIONS_TX=uc2_filtered_transactions
      - UC2_TRANSACTIONS_SHARDS={MAX_AMOUNT_GROUPBYS}
      - UC3_PERIOD_A_TRANSACTIONS_TX=uc3_transactons_period_A
      - UC3_PERIOD_B_TRANSACTIONS_TX=uc3_transactons_period_B
      - UC3_PERIOD_A_SHARDS={UC3_GROUP_BYS}
      - UC3_PERIOD_B_SHARDS={UC3_MERGES}
      - UC4_TRANSACTIONS_TX=uc4_filtered_transactions
      - UC4_TRANSACTIONS_SHARDS={UC4_COMPUTE_GRAPHS}
      - UC4_DEGREE_TRANSACTIONS_TX=uc4_degree_transactions
      - UC4_DEGREE_TRANSACTIONS_SHARDS={UC4_DEGREE_COMPUTE_GRAPHS}
      - UC5_TRANSACTIONS_TX=uc5_filtered_transactions
      - UC5_TRANSACTIONS_SHARDS={UC5_CONVERTERS}
      - IDX={idx}
      - NPEERS={DEFAULT_FILTERS}
      - RING_NAME=default_filter_ring
      - STRATEGY=default
      - STATE_DIR=/state
      - CHECKPOINT_EVERY={CHECKPOINT_EVERY}
    volumes:
      - ./state/default_filter_{idx}:/state"""

    return compose
