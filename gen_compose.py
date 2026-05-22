#!/usr/bin/env python3
"""
Regenerate docker-compose.yaml from the worker counts below.
Usage: python gen_compose.py
"""

# --- CONFIG ---
FILTER_WORKERS = 5
# --------------


def _on_rabbitmq():
    return """\
    depends_on:
      rabbitmq:
        condition: service_healthy"""


def _on_gateway():
    return """\
    depends_on:
      - gateway"""


def _filter(i):
    return f"""\
  filter_{i}:
    build:
      context: ./src/
      dockerfile: filter/Dockerfile
    container_name: filter_{i}
{_on_rabbitmq()}
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - TRANSACTIONS_RX=transactions
      - FILTERED_TX=filtered_transactions
      - UC2_TRANSACTIONS_TX=uc2_transactions
      - UC5_TRANSACTIONS_TX=uc5_transactions
      - FILTER_ID={i}
      - FILTER_RING_BASE=filter_ring
      - FILTER_WORKERS={FILTER_WORKERS}"""


def _client(i):
    return f"""\
  client_{i}:
    build:
      context: ./src/
      dockerfile: client/Dockerfile
    container_name: client_{i}
{_on_gateway()}
    environment:
      - TRANSACTIONS_PATH=/datasets/transactions_{i}.csv
      - ACCOUNTS_PATH=/datasets/LI-Small_accounts.csv
      - RESPONSES_PATH=/responses/responses_{i}.csv
      - GATEWAY_HOST=gateway
      - GATEWAY_PORT=12345
      - NRESPONSES=3
    volumes:
      - ./datasets:/datasets
      - ./responses:/responses"""


FIXED = """\
services:
  rabbitmq:
    build:
      context: ./src/rabbitmq
      dockerfile: Dockerfile
    container_name: rabbitmq
    environment:
      - RABBITMQ_LOG_LEVELS=error
    healthcheck:
      interval: 5s
      retries: 10
      start_period: 50s
      test: rabbitmq-diagnostics check_port_connectivity
      timeout: 3s
    ports:
      - 5672:5672
      - 15672:15672
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
      - SERVER_QUEUE_RX=server_responses
      - TRANSACTIONS_TX=transactions
      - ACCOUNTS_TX=accounts_tx"""

FIXED_UC1 = """\
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
      - STRATEGY=uc1
      - CLIENT_RESPONSES_RX=filtered_transactions
      - CLIENT_RESPONSES_TX=server_responses"""

FIXED_UC2 = """\
  uc2_group_by_trans:
    build:
      context: ./src/
      dockerfile: group_by/Dockerfile
    container_name: uc2_group_by_trans
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - STRATEGY=uc2_max_amount
      - RX=uc2_transactions
      - TX=partial_max_amounts
  uc2_aggregate_trans:
    build:
      context: ./src/
      dockerfile: aggregate/Dockerfile
    container_name: uc2_aggregate_trans
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - STRATEGY=uc2_max_amount
      - RX=partial_max_amounts
      - TX=max_amounts_by_bank
  uc2_group_by_accs:
    build:
      context: ./src/
      dockerfile: group_by/Dockerfile
    container_name: uc2_group_by_accs
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - STRATEGY=uc2_bank_names
      - RX=accounts_tx
      - TX=partial_bank_names
  uc2_aggregate_accs:
    build:
      context: ./src/
      dockerfile: aggregate/Dockerfile
    container_name: uc2_aggregate_accs
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - STRATEGY=uc2_bank_names
      - RX=partial_bank_names
      - TX=bank_id_name_mappings
  uc2_merge:
    build:
      context: ./src/
      dockerfile: merge/Dockerfile
    container_name: uc2_merge
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - LEFT_RX=max_amounts_by_bank
      - RIGHT_RX=bank_id_name_mappings
      - TX=max_amounts_w_name
  uc2_join:
    build:
      context: ./src/
      dockerfile: join/Dockerfile
    container_name: uc2_join
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - STRATEGY=uc2
      - CLIENT_RESPONSES_RX=max_amounts_w_name
      - CLIENT_RESPONSES_TX=server_responses"""

FIXED_UC5 = """\
  uc5_converter:
    build:
      context: ./src/
      dockerfile: converter/Dockerfile
    container_name: uc5_converter
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - STRATEGY=uc5_usd
      - RX=uc5_transactions
      - TX=uc5_converted
  uc5_amount_filter:
    build:
      context: ./src/
      dockerfile: filter/Dockerfile
    container_name: uc5_amount_filter
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - STRATEGY=uc5_amount
      - FILTER_ID=0
      - FILTER_RING_BASE=uc5_amount_filter_ring
      - FILTER_WORKERS=1
      - TRANSACTIONS_RX=uc5_converted
      - UC5_AMOUNT_FILTERED_TX=uc5_filtered
  uc5_group_by_count:
    build:
      context: ./src/
      dockerfile: group_by/Dockerfile
    container_name: uc5_group_by_count
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - STRATEGY=uc5_count
      - RX=uc5_filtered
      - TX=uc5_partial_counts
  uc5_join:
    build:
      context: ./src/
      dockerfile: join/Dockerfile
    container_name: uc5_join
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - STRATEGY=uc5
      - CLIENT_RESPONSES_RX=uc5_partial_counts
      - CLIENT_RESPONSES_TX=server_responses"""


def generate():
    sections = [
        FIXED,
        "\n".join(_filter(i) for i in range(FILTER_WORKERS)),
        FIXED_UC1,
        FIXED_UC2,
        FIXED_UC5,
        _client(0),
        _client(1),
    ]
    return "\n".join(sections) + "\n"


if __name__ == "__main__":
    output = generate()
    with open("docker-compose.yaml", "w") as f:
        f.write(output)
