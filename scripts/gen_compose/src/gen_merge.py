def _merge_service(
    name: str,
    container: str,
    strategy: str,
    left_rx_name: str,
    right_rx_name: str,
    tx_name: str,
    naffinity_downstream: int,
    checkpoint_every: int | None,
    idx: int | None = None,
    npeers: int | None = None,
    ring_name: str | None = None,
) -> str:
    compose = f"""\n
  {container}:
    build:
      context: ./src/
      dockerfile: merge/Dockerfile
    container_name: {container}
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - LEFT_RX={left_rx_name}
      - RIGHT_RX={right_rx_name}
      - TX={tx_name}
      - STRATEGY={strategy}
      - NAFFINITY_DOWNSTREAM={naffinity_downstream}"""

    if npeers is not None:
        compose += f"""
      - IDX={idx}
      - NPEERS={npeers}
      - RING_NAME={ring_name}"""

    if checkpoint_every is not None:
        from .gen_nodes import CHECKPOINT_EVERY

        compose += f"""
      - STATE_DIR=/state
      - CHECKPOINT_EVERY={CHECKPOINT_EVERY}
    volumes:
      - ./state/{container}:/state"""

    return compose


def gen_merge(
    strategy: str,
    left_rx_name: str,
    right_rx_name: str,
    tx_name: str,
    checkpoint_every: int | None = None,
    naffinity_downstream: int = 0,
    npeers: int = 1,
):
    name = strategy
    ring_name = f"{name}_ring"
    compose = ""
    for idx in range(npeers):
        compose += _merge_service(
            name, f"{name}_{idx}", strategy, left_rx_name, right_rx_name, tx_name,
            naffinity_downstream, checkpoint_every,
            idx=idx, npeers=npeers, ring_name=ring_name,
        )
    return compose
