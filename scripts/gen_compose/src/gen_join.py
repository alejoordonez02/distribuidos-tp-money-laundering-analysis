from .common_queues import RESPONSES, UC1_JOIN, UC2_JOIN, UC3_JOIN, UC4_JOIN, UC5_JOIN


def gen_join():
    return f"""\n
# === join ===

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
      - UC1_RX={UC1_JOIN}
      - UC2_RX={UC2_JOIN}
      - UC3_RX={UC3_JOIN}
      - UC4_RX={UC4_JOIN}
      - UC5_RX={UC5_JOIN}
      - RESPONSES_TX={RESPONSES}"""
