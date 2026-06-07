from .container_type import ContainerType


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
