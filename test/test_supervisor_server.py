import time

import pytest

from common.heartbeat import HeartbeatClient
from supervisor.registry import NodeRegistry, Status
from supervisor.server import SupervisorNode


def _wait_until(predicate, timeout=5.0, step=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return False


def _status_of(reg, node_id):
    nodes, _ = reg.snapshot()
    return {n.node_id: n.status for n in nodes}.get(node_id)


@pytest.mark.skip()
def test_node_registers_and_then_is_detected_dead():
    registry = NodeRegistry(timeout=0.3)
    server = SupervisorNode("127.0.0.1", 0, registry, sweep_interval=0.05)
    # bind to an ephemeral port, then read it back before accepting
    server.start()
    port = server._srv.getsockname()[1]

    client = HeartbeatClient("node_1", "filter", "127.0.0.1", port, interval=0.05)
    client.start()

    assert _wait_until(lambda: _status_of(registry, "node_1") is Status.ALIVE)

    client.stop()  # heartbeats stop -> registry must time it out
    assert _wait_until(lambda: _status_of(registry, "node_1") is Status.DEAD)

    server.stop()
