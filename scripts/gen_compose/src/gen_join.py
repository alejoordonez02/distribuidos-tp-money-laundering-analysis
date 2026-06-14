from . import topology as topo
from .common_queues import RESPONSES, UC1_JOIN, UC2_JOIN, UC3_JOIN, UC4_JOIN, UC5_JOIN
from .gen_nodes import CHECKPOINT_EVERY
from .runtime import restart_line
from .supervisor_env import supervisor_env


def gen_join():
    compose = "\n# === join ==="
    for idx, uc_ids in enumerate(topo.JOIN_PARTITION):
        compose += _join_service(idx, uc_ids)
    return compose


def _join_service(idx: int, uc_ids: list[int]) -> str:
    join_ucs = ",".join(str(u) for u in uc_ids)
    sup = supervisor_env(f"join_{idx}", "join")
    return f"""\n
  join_{idx}:
    build:
      context: ./src/
      dockerfile: join/Dockerfile
    container_name: join_{idx}{restart_line()}
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
      - JOIN_UCS={join_ucs}
      - RESPONSES_TX={RESPONSES}
      - STATE_DIR=/state
      - CHECKPOINT_EVERY={CHECKPOINT_EVERY}{sup}
    volumes:
      - ./state/join_{idx}:/state"""
