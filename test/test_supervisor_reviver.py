from supervisor.registry import NodeRegistry
from supervisor.reviver import Reviver


def test_revives_only_dead_nodes():
    reg = NodeRegistry(timeout=5)
    reg.register("alive_1", "filter", now=0.0)
    reg.register("dead_1", "filter", now=0.0)
    reg.check_timeouts(now=100.0)  # both dead...
    reg.heartbeat("alive_1", now=101.0)  # ...but alive_1 recovered

    started = []
    Reviver(reg, start_fn=started.append).sweep(now=200.0)

    assert started == ["dead_1"]


def test_cooldown_prevents_repeat_revives():
    reg = NodeRegistry(timeout=5)
    reg.register("dead_1", "filter", now=0.0)
    reg.check_timeouts(now=100.0)

    started = []
    r = Reviver(reg, cooldown=15.0, start_fn=started.append)
    r.sweep(now=200.0)        # first attempt
    r.sweep(now=205.0)        # within cooldown -> skipped
    r.sweep(now=230.0)        # past cooldown -> retried
    assert started == ["dead_1", "dead_1"]


def test_revive_is_logged_as_event():
    reg = NodeRegistry(timeout=5)
    reg.register("dead_1", "filter", now=0.0)
    reg.check_timeouts(now=100.0)
    Reviver(reg, start_fn=lambda _: None).sweep(now=200.0)
    _, events = reg.snapshot()
    assert any("reviving" in e.message and e.node_id == "dead_1" for e in events)
