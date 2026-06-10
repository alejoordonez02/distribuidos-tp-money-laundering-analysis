def gen_merge(
    strategy: str,
    left_rx_name: str,
    right_rx_name: str,
    tx_name: str,
    checkpoint_every: int | None = None,
):
    name = strategy
    compose = f"""\n
  {name}:
    build:
      context: ./src/
      dockerfile: merge/Dockerfile
    container_name: {name}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - LEFT_RX={left_rx_name}
      - RIGHT_RX={right_rx_name}
      - TX={tx_name}
      - STRATEGY={strategy}"""

    if checkpoint_every is not None:
        from .gen_nodes import CHECKPOINT_EVERY

        compose += f"""
      - STATE_DIR=/state
      - CHECKPOINT_EVERY={CHECKPOINT_EVERY}
    volumes:
      - ./state/{name}:/state"""

    return compose
