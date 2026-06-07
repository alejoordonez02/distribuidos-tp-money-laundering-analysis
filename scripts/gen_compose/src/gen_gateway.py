def gen_gateway():
    return """
  gateway:
    build:
      context: ./src/
      dockerfile: gateway/Dockerfile
    container_name: gateway
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - GATEWAY_HOST=gateway
      - GATEWAY_PORT=12345
      - MOM_HOST=rabbitmq
      - MOM_PORT=5672
      - SERVER_QUEUE_RX=responses
      - TRANSACTIONS_TX=client_transactions
      - ACCOUNTS_TX=client_accounts
      """
