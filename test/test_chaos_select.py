import random

from chaos.main import select_victims


def test_excludes_excluded_and_infra():
    # supervisor_0/_1 are the cluster's replica containers: a base name in the exclude
    # set must protect them too, otherwise the chaos monkey kills the supervision.
    cands = ["filter_0", "rabbitmq", "supervisor_0", "supervisor_1", "gateway", "client_0", "chaos", "join_0"]
    exclude = {"rabbitmq", "supervisor", "gateway", "chaos"}
    out = select_victims(cands, exclude, kills=10, rng=random.Random(0))
    assert set(out) == {"filter_0", "join_0"}  # supervisor_N by base name, client_0 by prefix


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
