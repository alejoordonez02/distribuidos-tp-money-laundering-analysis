from .fault_tolerance import HEARTBEAT_INTERVAL

SUPERVISOR_HOST = "supervisor"
SUPERVISOR_PORT = 9100

# Each node registers here to seed the supervisors' expected set: a leader elected after a crash can revive a node it never heard a heartbeat from.
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


def state_volumes_block() -> str:
    """Top-level compose `volumes:` section declaring one named volume per node, so each
    node owns its state instead of a shared host bind mount (multi-computer model). Must be
    read AFTER all node services were generated so the list is complete."""
    if not _SUPERVISED_NODES:
        return ""
    decls = "\n".join(f"  {name}_state:" for name in _SUPERVISED_NODES)
    return f"\n\nvolumes:\n{decls}\n"
