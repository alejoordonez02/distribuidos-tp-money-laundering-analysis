from .container_type import ContainerType


# TODO: strategy debería ser el nombre pero
#       alta paja cambiarlo ahora en todos
#       lados
def gen_nodes(
    type2: ContainerType,
    strategy: str,
    npeers: int,
    affinity_upstream: bool,
    naffinity_downstream: int,
    rx_name: str,
    tx_name: str,
) -> str:
    """
    Gen a ring of nodes.

    # Args
    * `type2` - the type of the controller.
    * `name` - the name of the container.
    * `strategy` - the strategy for the controller to use.
    * `npeers` - the amount of nodes in the ring, including this one.
    * `affinity_upstream` - wheter the controller is supposed to
      expect upstream messages with affinity routing.
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
      - PYTHONHASHSEED=2026"""

    return compose
