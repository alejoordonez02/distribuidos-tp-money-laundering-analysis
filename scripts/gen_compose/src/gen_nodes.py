from enum import StrEnum

from .container_type import ContainerType

CHECKPOINT_EVERY = 1000


def gen_nodes(
    type2: ContainerType,
    strategy: StrEnum,
    npeers: int,
    naffinity_downstream: int,
    rx_name: str,
    tx_name: str,
    checkpoint_every: int | None = None,
    broadcast_downstream: bool = False,
) -> str:
    name = strategy
    compose = ""

    for idx in range(npeers):
        compose += f"""\n
  {name}_{idx}:
    build:
      context: ./src/
      dockerfile: {type2}/Dockerfile
    container_name: {name}_{idx}
    restart: on-failure
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
      - NAFFINITY_DOWNSTREAM={naffinity_downstream}
      - BROADCAST_DOWNSTREAM={1 if broadcast_downstream else 0}
      - PYTHONHASHSEED=2026"""

        if checkpoint_every is not None:
            compose += f"""
      - STATE_DIR=/state
      - CHECKPOINT_EVERY={CHECKPOINT_EVERY}
    volumes:
      - ./state/{name}_{idx}:/state"""

    return compose
