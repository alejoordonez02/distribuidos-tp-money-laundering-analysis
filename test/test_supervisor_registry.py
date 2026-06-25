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


def test_expected_node_never_seen_is_marked_dead_after_grace():
    # A leader elected after a crash must revive a node that was already dead at
    # the transition and therefore never sends THIS leader a heartbeat. The grace
    # is measured from the first sweep, not from t=0.
    reg = NodeRegistry(timeout=5, expected=["n1"])
    reg.check_timeouts(now=100.0)  # sweep starts -> grace begins
    assert _status_of(reg, "n1") is Status.UNKNOWN  # still within grace
    reg.check_timeouts(now=106.0)  # past the grace
    assert _status_of(reg, "n1") is Status.DEAD


def test_expected_node_that_registers_is_not_killed_as_never_seen():
    reg = NodeRegistry(timeout=5, expected=["n1"])
    reg.check_timeouts(now=100.0)  # grace begins
    reg.register("n1", "filter", now=103.0)  # reports in
    reg.check_timeouts(now=107.0)  # 7s since grace start, but node is ALIVE & fresh
    assert _status_of(reg, "n1") is Status.ALIVE


def test_transitions_are_logged_as_events():
    reg = NodeRegistry(timeout=5)
    reg.register("n1", "filter", now=0.0)
    reg.check_timeouts(now=10.0)
    _, events = reg.snapshot()
    msgs = [e.message for e in events]
    assert any("registered" in m for m in msgs)
    assert any("heartbeat lost" in m for m in msgs)
