from uuid import uuid4

from common.checkpoint.persistent_spill import PersistentSpill

C = uuid4()


def test_append_and_read(tmp_path):
    s = PersistentSpill(str(tmp_path), "t")
    s.append(C, "a\n")
    s.append(C, "b\n")
    assert list(s.iter_lines_and_clear(C)) == ["a\n", "b\n"]


def test_read_clears_the_file(tmp_path):
    s = PersistentSpill(str(tmp_path), "t")
    s.append(C, "a\n")
    list(s.iter_lines_and_clear(C))
    assert list(s.iter_lines_and_clear(C)) == []


def test_survives_new_instance_same_dir(tmp_path):
    s1 = PersistentSpill(str(tmp_path), "t")
    s1.append(C, "a\n")
    s1.snapshot_state()

    s2 = PersistentSpill(str(tmp_path), "t")
    assert list(s2.iter_lines_and_clear(C)) == ["a\n"]


def test_restore_truncates_uncommitted_appends(tmp_path):
    s1 = PersistentSpill(str(tmp_path), "t")
    s1.append(C, "committed\n")
    snap = s1.snapshot_state()
    s1.append(C, "uncommitted\n")  # written after checkpoint, then crash

    s2 = PersistentSpill(str(tmp_path), "t")
    s2.restore_state(snap)
    s2.append(C, "redelivered\n")  # re-appended on reprocess

    assert list(s2.iter_lines_and_clear(C)) == ["committed\n", "redelivered\n"]
