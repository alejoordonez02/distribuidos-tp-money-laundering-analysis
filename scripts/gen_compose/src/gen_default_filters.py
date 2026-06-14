from . import topology as topo
from .common_queues import (
    CLIENT_TRANSACTIONS,
    UC1_JOIN,
    UC2_FILTERED_TRANSACTIONS,
    UC3_PERIOD_A_TRANSACTIONS,
    UC3_PERIOD_B_TRANSACTIONS,
    UC4_DEGREE_TRANSACTIONS,
    UC4_TRANSACTIONS,
    UC5_TRANSACTIONS,
)
from .gen_nodes import CHECKPOINT_EVERY
from .runtime import restart_line
from .supervisor_env import supervisor_env

_ROUTE_SHARDS = {
    "UC1_TRANSACTIONS_SHARDS": 0,
    "UC2_TRANSACTIONS_SHARDS": topo.UC2_MAX_AMOUNT_GROUP_BYS,
    "UC3_PERIOD_A_SHARDS": topo.UC3_GROUP_BYS,
    "UC3_PERIOD_B_SHARDS": topo.UC3_MERGES,
    "UC4_TRANSACTIONS_SHARDS": topo.UC4_COMPUTE_GRAPHS,
    "UC4_DEGREE_TRANSACTIONS_SHARDS": topo.UC4_DEGREE_COMPUTE_GRAPHS,
    "UC5_TRANSACTIONS_SHARDS": topo.UC5_CONVERTERS,
}


def gen_default_filters() -> str:
    compose = "\n# === default filters ==="

    for idx in range(topo.DEFAULT_FILTERS):
        sup = supervisor_env(f"default_filter_{idx}", "filter")
        compose += f"""\n
  default_filter_{idx}:
    build:
      context: ./src/
      dockerfile: filter/Dockerfile
    container_name: default_filter_{idx}{restart_line()}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - RX={CLIENT_TRANSACTIONS}
      - UC1_TRANSACTIONS_TX={UC1_JOIN}
      - UC2_TRANSACTIONS_TX={UC2_FILTERED_TRANSACTIONS}
      - UC3_PERIOD_A_TRANSACTIONS_TX={UC3_PERIOD_A_TRANSACTIONS}
      - UC3_PERIOD_B_TRANSACTIONS_TX={UC3_PERIOD_B_TRANSACTIONS}
      - UC4_TRANSACTIONS_TX={UC4_TRANSACTIONS}
      - UC4_DEGREE_TRANSACTIONS_TX={UC4_DEGREE_TRANSACTIONS}
      - UC5_TRANSACTIONS_TX={UC5_TRANSACTIONS}
      - UC1_TRANSACTIONS_SHARDS={_ROUTE_SHARDS["UC1_TRANSACTIONS_SHARDS"]}
      - UC2_TRANSACTIONS_SHARDS={_ROUTE_SHARDS["UC2_TRANSACTIONS_SHARDS"]}
      - UC3_PERIOD_A_SHARDS={_ROUTE_SHARDS["UC3_PERIOD_A_SHARDS"]}
      - UC3_PERIOD_B_SHARDS={_ROUTE_SHARDS["UC3_PERIOD_B_SHARDS"]}
      - UC4_TRANSACTIONS_SHARDS={_ROUTE_SHARDS["UC4_TRANSACTIONS_SHARDS"]}
      - UC4_DEGREE_TRANSACTIONS_SHARDS={_ROUTE_SHARDS["UC4_DEGREE_TRANSACTIONS_SHARDS"]}
      - UC5_TRANSACTIONS_SHARDS={_ROUTE_SHARDS["UC5_TRANSACTIONS_SHARDS"]}
      - IDX={idx}
      - NPEERS={topo.DEFAULT_FILTERS}
      - RING_NAME=default_filter_ring
      - STRATEGY=default
      - STATE_DIR=/state
      - CHECKPOINT_EVERY={CHECKPOINT_EVERY}{sup}
    volumes:
      - ./state/default_filter_{idx}:/state"""

    return compose
