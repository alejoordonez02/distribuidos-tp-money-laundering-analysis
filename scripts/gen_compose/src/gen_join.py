from .common_queues import RESPONSES, UC1_JOIN, UC2_JOIN, UC3_JOIN, UC4_JOIN, UC5_JOIN
from .gen_nodes import CHECKPOINT_EVERY

# How the join scales: the join is N independent per-UC handlers (own queue, own
# state, own thread) with nothing shared but the responses queue, so we partition
# those handlers across containers. THIS is the single place that declares which UC
# goes to which container — one inner list per replica. Grouped by the nature of the
# work, not round-robin: UC1 and UC3 each spill ALL transactions (the heavy I/O), so
# each gets its own container; the lighter joins (UC2 max-by-bank, UC4 paths, UC5
# count) share the third. Edit this to repartition, add a UC to a group, or add a
# container — the join scales by changing this list alone.
JOIN_PARTITION = [
    [1],        # join_0: UC1  (spills every transaction)
    [3],        # join_1: UC3  (spills every transaction)
    [2, 4, 5],  # join_2: the lighter joins
]


def gen_join():
    compose = "\n# === join ==="
    for idx, uc_ids in enumerate(JOIN_PARTITION):
        compose += _join_service(idx, uc_ids)
    return compose


def _join_service(idx: int, uc_ids: list[int]) -> str:
    join_ucs = ",".join(str(u) for u in uc_ids)
    return f"""\n
  join_{idx}:
    build:
      context: ./src/
      dockerfile: join/Dockerfile
    container_name: join_{idx}
    restart: on-failure
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
      - CHECKPOINT_EVERY={CHECKPOINT_EVERY}
    volumes:
      - ./state/join_{idx}:/state"""
