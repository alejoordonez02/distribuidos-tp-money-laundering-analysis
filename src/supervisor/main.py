import logging
import os
import threading

from common.graceful_shutdown import setup_graceful_shutdown

from .registry import NodeRegistry
from .reviver import Reviver
from .server import Peer, SupervisorNode
from .tui import Dashboard


def main() -> None:
    logging.basicConfig(level=os.getenv("LOGGING_LEVEL", "WARNING"))
    logging.getLogger("pika").setLevel(logging.WARNING)

    bind_host = os.getenv("SUPERVISOR_BIND", "0.0.0.0")
    server_port = int(os.getenv("SUPERVISOR_PORT", "9100"))
    internal_port = int(os.getenv("INTERNAL_PORT", "9100"))
    leader_port = int(os.getenv("LEADER_PORT", "9100"))
    idx = int(os.getenv("IDX", "0"))
    nnodes = int(os.getenv("NNODES", "1"))
    node_prefix = os.getenv("NODE_PREFIX", "supervisor_node-")
    timeout = float(os.getenv("HEARTBEAT_TIMEOUT", "6"))
    expected = [n for n in os.getenv("EXPECTED_NODES", "").split(",") if n]
    # 0 disables revival (detection only); the reviver needs the docker socket.
    revive_interval = float(os.getenv("REVIVE_INTERVAL", "5"))

    peers = [Peer(i, f"{node_prefix}{i}") for i in range(nnodes) if i != idx]

    registry = NodeRegistry(timeout, expected=expected)
    server = SupervisorNode(
        idx, bind_host, server_port, internal_port, leader_port, peers, registry
    )
    dashboard = Dashboard(registry)
    reviver = (
        Reviver(registry, interval=revive_interval) if revive_interval > 0 else None
    )

    stop = threading.Event()

    def shutdown() -> None:
        stop.set()
        server.stop()

    setup_graceful_shutdown(shutdown)
    server.start()
    if reviver is not None:
        threading.Thread(
            target=reviver.run, args=(stop,), name="reviver", daemon=True
        ).start()
    dashboard.run(stop)


if __name__ == "__main__":
    main()
