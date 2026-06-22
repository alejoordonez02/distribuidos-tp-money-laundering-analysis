from .fault_tolerance import HEARTBEAT_TIMEOUT, REVIVE_INTERVAL
from .supervisor_env import SUPERVISOR_HOST, SUPERVISOR_PORT

SUPERVISOR_LOGGING_LEVEL = "DEBUG"
SUPERVISOR_BIND = "0.0.0.0"
SUPERVISOR_INTERNAL_PORT = 9101
SUPERVISOR_LEADER_PORT = 9102
NSUPERVISORS = 1
SUPERVISOR_PREFIX = "supervisor_"


def _gen_supervisor(idx: int, nnodes: int):
    container_name = f"{SUPERVISOR_PREFIX}{idx}"
    return f"""\
  {container_name}:
    build:
      context: ./src/
      dockerfile: supervisor/Dockerfile
    container_name: {container_name}
    restart: on-failure
    tty: true
    stdin_open: true
    environment:
      - SUPERVISOR_BIND={SUPERVISOR_BIND}
      - SUPERVISOR_PORT={SUPERVISOR_PORT}
      - INTERNAL_PORT={SUPERVISOR_INTERNAL_PORT}
      - LEADER_PORT={SUPERVISOR_LEADER_PORT}
      - IDX={idx}
      - NNODES={nnodes}
      - NODE_PREFIX={SUPERVISOR_PREFIX}
      - HEARTBEAT_TIMEOUT=${{HEARTBEAT_TIMEOUT:-{HEARTBEAT_TIMEOUT}}}
      - REVIVE_INTERVAL=${{REVIVE_INTERVAL:-{REVIVE_INTERVAL}}}
      - LOGGING_LEVEL={SUPERVISOR_LOGGING_LEVEL}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    networks:
      default:
        aliases:
          - {SUPERVISOR_HOST}"""


def gen_supervisors() -> str:
    return f"""\n
# === supervisor ===
{"\n\n".join(_gen_supervisor(i, NSUPERVISORS) for i in range(NSUPERVISORS))}"""
