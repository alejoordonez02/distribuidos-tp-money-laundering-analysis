from .fault_tolerance import HEARTBEAT_INTERVAL

SUPERVISOR_HOST = "supervisor"
SUPERVISOR_PORT = 9100

# Every node that wires a heartbeat client registers its name here, so the
# supervisors can be seeded with the full expected set (see expected_nodes_csv).
# A leader elected after a crash thus knows a node exists even if that node is
# already dead and never sends it a heartbeat -> it can still be revived.
_SUPERVISED_NODES: list[str] = []


def supervisor_env(node_name: str, kind: str) -> str:
    """Env block pointing a node's heartbeat client at the supervisor. Injected
    into every node service so liveness is reported uniformly."""
    if node_name not in _SUPERVISED_NODES:
        _SUPERVISED_NODES.append(node_name)
    return f"""
      - SUPERVISOR_HOST={SUPERVISOR_HOST}
      - SUPERVISOR_PORT={SUPERVISOR_PORT}
      - NODE_NAME={node_name}
      - NODE_KIND={kind}
      - HEARTBEAT_INTERVAL=${{HEARTBEAT_INTERVAL:-{HEARTBEAT_INTERVAL}}}"""


def expected_nodes_csv() -> str:
    """Comma-separated list of every supervised node, in declaration order. Must
    be read AFTER all node services were generated so the list is complete."""
    return ",".join(_SUPERVISED_NODES)
