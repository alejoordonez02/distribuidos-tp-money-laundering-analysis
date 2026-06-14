from .fault_tolerance import HEARTBEAT_INTERVAL

SUPERVISOR_HOST = "supervisor"
SUPERVISOR_PORT = 9100


def supervisor_env(node_name: str, kind: str) -> str:
    """Env block pointing a node's heartbeat client at the supervisor. Injected
    into every node service so liveness is reported uniformly."""
    return f"""
      - SUPERVISOR_HOST={SUPERVISOR_HOST}
      - SUPERVISOR_PORT={SUPERVISOR_PORT}
      - NODE_NAME={node_name}
      - NODE_KIND={kind}
      - HEARTBEAT_INTERVAL=${{HEARTBEAT_INTERVAL:-{HEARTBEAT_INTERVAL}}}"""
