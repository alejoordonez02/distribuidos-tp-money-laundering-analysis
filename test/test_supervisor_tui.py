from supervisor.registry import NodeRegistry
from supervisor.tui import Dashboard


def test_render_shows_nodes_and_counts():
    reg = NodeRegistry(timeout=5, expected=["n2"])
    reg.register("n1", "filter", now=0.0)
    out = Dashboard(reg).render()
    assert "n1" in out
    assert "filter" in out
    assert "alive=1" in out
    assert "n2" in out  # expected-but-unregistered node still shown
