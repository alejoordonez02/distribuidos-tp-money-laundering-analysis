from .supervisor_env import SUPERVISOR_PORT

# A node is considered dead after this long without a heartbeat. A few heartbeat
# intervals wide (heartbeats are every 1s) so a momentary hiccup is not a crash.
HEARTBEAT_TIMEOUT = 3
# How often the supervisor scans for dead nodes to revive (0 = detection only).
REVIVE_INTERVAL = 2


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
