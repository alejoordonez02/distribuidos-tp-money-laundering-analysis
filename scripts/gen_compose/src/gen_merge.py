def gen_merge(
    name: str,
    strategy: str,
    left_rx_name: str,
    right_rx_name: str,
    tx_name: str,
):
    return f"""
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
      - STRATEGY={strategy}
      """
