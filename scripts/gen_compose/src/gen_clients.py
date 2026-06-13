import os

from scripts.cfg import ACCOUNTS_PATH, NCLIENTS  # type: ignore[reportMissingImports]

_ACCOUNTS_FILE = os.path.basename(ACCOUNTS_PATH)

_CLIENT_TEMPLATE = """\n
  client_{n}:
    build:
      context: ./src/
      dockerfile: client/Dockerfile
    container_name: client_{n}
    depends_on:
      - gateway
    environment:
      - TRANSACTIONS_PATH=/datasets/transactions_{n}.csv
      - ACCOUNTS_PATH=/datasets/{accounts}
      - RESPONSES_PATH=/responses/responses_{n}.csv
      - GATEWAY_HOST=gateway
      - GATEWAY_PORT=12345
      - NRESPONSES=5
      - BATCH_SIZE=500
    volumes:
      - ./datasets:/datasets
      - ./responses:/responses"""

OUTPUT_PATH = "docker-compose.clients.yaml"


def gen_clients():
    compose = "\n# === clients ==="
    compose += "".join(
        _CLIENT_TEMPLATE.format(n=n, accounts=_ACCOUNTS_FILE) for n in range(NCLIENTS)
    )
    return compose
