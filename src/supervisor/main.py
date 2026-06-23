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
    internal_port = int(os.getenv("INTERNAL_PORT", "9101"))
    leader_port = int(os.getenv("LEADER_PORT", "9102"))
    idx = int(os.getenv("IDX", "0"))
    nnodes = int(os.getenv("NNODES", "1"))
    node_prefix = os.getenv("NODE_PREFIX", "supervisor_")
    timeout = float(os.getenv("HEARTBEAT_TIMEOUT", "6"))
    expected = [n for n in os.getenv("EXPECTED_NODES", "").split(",") if n]
    # 0 disables revival (detection only); the reviver needs the docker socket.
    sweep_interval = float(os.getenv("SWEEP_INTERVAL", "0.5"))
    ping_delay = float(os.getenv("PING_DELAY", "0.5"))
    revive_interval = float(os.getenv("REVIVE_INTERVAL", "5"))

    peers = [Peer(i, f"{node_prefix}{i}") for i in range(nnodes) if i != idx]

    def registry_factory():
        return NodeRegistry(timeout, expected=expected)

    def reviver_factory(registry: NodeRegistry):
        return (
            Reviver(registry, interval=revive_interval) if revive_interval > 0 else None
        )

    def dashboard_factory(registry: NodeRegistry):
        return Dashboard(registry)

    server = SupervisorNode(
        idx,
        bind_host,
        server_port,
        internal_port,
        leader_port,
        peers,
        registry_factory,
        reviver_factory,
        dashboard_factory,
        sweep_interval,
        ping_delay,
    )

    stop = threading.Event()

    def shutdown() -> None:
        stop.set()
        server.stop()

    setup_graceful_shutdown(shutdown)
    server.start()
    stop.wait()


if __name__ == "__main__":
    main()
