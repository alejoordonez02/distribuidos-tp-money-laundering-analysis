def gen_join():
    return """
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
      - UC1_RX=uc1_join
      - UC2_RX=uc2_join
      - UC3_RX=uc3_join
      - UC4_RX=uc4_join
      - UC5_RX=uc5_join
      - RESPONSES_TX=responses
      """
