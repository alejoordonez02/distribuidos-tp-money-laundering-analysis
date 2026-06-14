from .fault_tolerance import HEARTBEAT_TIMEOUT, REVIVE_INTERVAL, SUPERVISOR_PORT


def gen_supervisor() -> str:
    return f"""\n
# === supervisor ===

  supervisor:
    build:
      context: ./src/
      dockerfile: supervisor/Dockerfile
    container_name: supervisor
    restart: on-failure
    tty: true
    stdin_open: true
    environment:
      - SUPERVISOR_BIND=0.0.0.0
      - SUPERVISOR_PORT={SUPERVISOR_PORT}
      - HEARTBEAT_TIMEOUT=${{HEARTBEAT_TIMEOUT:-{HEARTBEAT_TIMEOUT}}}
      - REVIVE_INTERVAL=${{REVIVE_INTERVAL:-{REVIVE_INTERVAL}}}
      - LOGGING_LEVEL=WARNING
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock"""
