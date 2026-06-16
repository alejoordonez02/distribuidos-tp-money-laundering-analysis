import os
from typing import Optional

from .client import HeartbeatClient


def make_heartbeat_client() -> Optional[HeartbeatClient]:
    """Build a heartbeat client from the environment, or None when no supervisor
    is configured — so the pipeline runs unchanged when there is no supervisor."""
    host = os.getenv("SUPERVISOR_HOST")
    if not host:
        return None
    port = int(os.getenv("SUPERVISOR_PORT", "9100"))
    node_id = os.getenv("NODE_NAME") or os.getenv("HOSTNAME", "unknown")
    kind = os.getenv("NODE_KIND", "node")
    interval = float(os.getenv("HEARTBEAT_INTERVAL", "2"))
    return HeartbeatClient(node_id, kind, host, port, interval)
