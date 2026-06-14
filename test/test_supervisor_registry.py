from supervisor.registry import NodeRegistry, Status


def _status_of(reg, node_id):
    nodes, _ = reg.snapshot()
    return {n.node_id: n.status for n in nodes}[node_id]


def test_register_marks_alive_with_kind():
    reg = NodeRegistry(timeout=5)
    reg.register("n1", "filter", now=0.0)
    nodes, _ = reg.snapshot()
    assert nodes[0].status is Status.ALIVE
    assert nodes[0].kind == "filter"


def test_timeout_marks_dead():
    reg = NodeRegistry(timeout=5)
    reg.register("n1", "filter", now=0.0)
    reg.check_timeouts(now=4.0)
    assert _status_of(reg, "n1") is Status.ALIVE
    reg.check_timeouts(now=6.0)
    assert _status_of(reg, "n1") is Status.DEAD


def test_heartbeat_recovers_dead_node():
    reg = NodeRegistry(timeout=5)
    reg.register("n1", "filter", now=0.0)
    reg.check_timeouts(now=10.0)
    assert _status_of(reg, "n1") is Status.DEAD
    reg.heartbeat("n1", now=11.0)
    assert _status_of(reg, "n1") is Status.ALIVE


def test_expected_nodes_start_unknown():
    reg = NodeRegistry(timeout=5, expected=["n1", "n2"])
    nodes, _ = reg.snapshot()
    assert {n.node_id: n.status for n in nodes} == {
        "n1": Status.UNKNOWN,
        "n2": Status.UNKNOWN,
    }


def test_transitions_are_logged_as_events():
    reg = NodeRegistry(timeout=5)
    reg.register("n1", "filter", now=0.0)
    reg.check_timeouts(now=10.0)
    _, events = reg.snapshot()
    msgs = [e.message for e in events]
    assert any("registered" in m for m in msgs)
    assert any("heartbeat lost" in m for m in msgs)
