import os

from common.checkpoint.checkpoint_store import CheckpointStore

P1 = b"\x01" * 16


def test_load_missing_returns_none(tmp_path):
    store = CheckpointStore(str(tmp_path / "node.ckpt"))
    assert store.load() is None


def test_save_then_load_round_trip(tmp_path):
    store = CheckpointStore(str(tmp_path / "node.ckpt"))
    blob = {"state": {"a": [1, 2.5]}, "dedup": {P1: 7}}

    store.save(blob)
    got = store.load()

    assert got["state"] == {"a": [1, 2.5]}
    assert got["dedup"][P1] == 7


def test_save_overwrites_atomically(tmp_path):
    path = str(tmp_path / "node.ckpt")
    store = CheckpointStore(path)

    store.save({"state": {"v": 1}, "dedup": {}})
    store.save({"state": {"v": 2}, "dedup": {}})

    assert store.load()["state"]["v"] == 2
    # no temp leftovers
    assert [f for f in os.listdir(tmp_path) if f.endswith(".tmp")] == []


def test_save_creates_missing_directory(tmp_path):
    store = CheckpointStore(str(tmp_path / "nested" / "dir" / "node.ckpt"))
    store.save({"state": {}, "dedup": {}})
    assert store.load() == {"state": {}, "dedup": {}}
