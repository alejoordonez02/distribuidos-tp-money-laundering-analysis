def gen_rabbitmq():
    return """\n
# === rabbitmq ===

  rabbitmq:
    build:
      context: ./src/rabbitmq
      dockerfile: Dockerfile
    container_name: rabbitmq
    environment:
      - RABBITMQ_LOG_LEVELS=error
      - RABBITMQ_CONFIG_FILE=rabbitmq.conf
    healthcheck:
      interval: 5s
      retries: 10
      start_period: 50s
      test: rabbitmq-diagnostics check_port_connectivity
      timeout: 3s
    ports:
      - 5672:5672
      - 15672:15672"""
