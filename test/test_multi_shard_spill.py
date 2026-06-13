from uuid import uuid4

from common.checkpoint.multi_shard_spill import MultiShardSpill

C = uuid4()


def test_append_and_read_per_shard(tmp_path):
    s = MultiShardSpill(str(tmp_path), "t")
    s.append(C, 0, "a\n")
    s.append(C, 1, "b\n")
    s.append(C, 0, "c\n")
    assert s.shards_of(C) == [0, 1]
    assert list(s.read_shard(C, 0)) == ["a\n", "c\n"]
    assert list(s.read_shard(C, 1)) == ["b\n"]


def test_clear_removes_all_shards(tmp_path):
    s = MultiShardSpill(str(tmp_path), "t")
    s.append(C, 0, "a\n")
    s.append(C, 7, "b\n")
    s.clear(C)
    assert s.shards_of(C) == []


def test_survives_new_instance(tmp_path):
    s1 = MultiShardSpill(str(tmp_path), "t")
    s1.append(C, 3, "x\n")
    s1.snapshot_state()

    s2 = MultiShardSpill(str(tmp_path), "t")
    assert s2.shards_of(C) == [3]
    assert list(s2.read_shard(C, 3)) == ["x\n"]


def test_restore_truncates_uncommitted(tmp_path):
    s1 = MultiShardSpill(str(tmp_path), "t")
    s1.append(C, 2, "committed\n")
    snap = s1.snapshot_state()
    s1.append(C, 2, "uncommitted\n")

    s2 = MultiShardSpill(str(tmp_path), "t")
    s2.restore_state(snap)
    s2.append(C, 2, "redelivered\n")

    assert list(s2.read_shard(C, 2)) == ["committed\n", "redelivered\n"]
