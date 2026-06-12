from enum import StrEnum

from .container_type import ContainerType

# Checkpoint/ack batch size (= how many messages a node holds + reprocesses on a
# crash). Higher amortizes the per-checkpoint fsync/spill cost; the rx prefetch is
# raised to match. `checkpoint_every` on the gen_* fns is the on/off switch.
CHECKPOINT_EVERY = 50


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
    """
    Gen a ring of nodes.

    # Args
    * `type2` - the type of the controller.
    * `name` - the name of the container.
    * `strategy` - the strategy for the controller to use.
    * `npeers` - the amount of nodes in the ring, including this one.
    * `naffinity_downstream` - the amount of downstream affinities
      ready to handle this controller's downstream messages. If none,
      this can be set to zero (which does not mean that there are not
      controllers waiting for messages on the other side).
    * `rx_name` - the name prefix of the upstream channel.
    * `tx_name` - the name prefix of the downstream channel.

    # Returns
    A string containing the nodes compose declaration.
    """

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
