from .common_queues import CLIENT_ACCOUNTS, CLIENT_TRANSACTIONS, RESPONSES
from .gen_default_filters import DEFAULT_FILTERS
from .gen_uc2 import BANK_NAMES_GROUPBYS


def gen_gateway():
    return f"""\n
# === gateway ===

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
      - SERVER_QUEUE_RX={RESPONSES}
      - TRANSACTIONS_TX={CLIENT_TRANSACTIONS}
      - ACCOUNTS_TX={CLIENT_ACCOUNTS}
      - NDEFAULT_FILTERS={DEFAULT_FILTERS}
      - NBANK_NAMES={BANK_NAMES_GROUPBYS}"""
