import random

from chaos.main import select_victims


def test_excludes_excluded_and_infra():
    cands = ["filter_0", "rabbitmq", "supervisor", "gateway", "client_0", "chaos", "join_0"]
    exclude = {"rabbitmq", "supervisor", "gateway", "chaos"}
    out = select_victims(cands, exclude, kills=10, rng=random.Random(0))
    assert set(out) == {"filter_0", "join_0"}  # client_0 dropped by prefix


def test_respects_kill_count():
    out = select_victims(["a", "b", "c", "d"], set(), kills=2, rng=random.Random(0))
    assert len(out) == 2


def test_deterministic_with_seed():
    cands = ["a", "b", "c", "d", "e"]
    assert select_victims(cands, set(), 3, random.Random(42)) == select_victims(
        cands, set(), 3, random.Random(42)
    )


def test_empty_pool_returns_nothing():
    assert select_victims(["client_0", "rabbitmq"], {"rabbitmq"}, 5, random.Random(0)) == []
