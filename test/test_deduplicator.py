from common.checkpoint.deduplicator import Deduplicator

P1 = b"\x01" * 16
P2 = b"\x02" * 16


def test_first_seq_is_not_duplicate():
    d = Deduplicator()
    assert not d.is_duplicate(P1, 1)


def test_seq_at_or_below_last_is_duplicate():
    d = Deduplicator()
    d.record(P1, 5)
    assert d.is_duplicate(P1, 5)
    assert d.is_duplicate(P1, 3)
    assert not d.is_duplicate(P1, 6)


def test_producers_are_independent():
    d = Deduplicator()
    d.record(P1, 10)
    assert d.is_duplicate(P1, 7)
    assert not d.is_duplicate(P2, 7)


def test_record_keeps_the_maximum():
    d = Deduplicator()
    d.record(P1, 5)
    d.record(P1, 3)
    assert d.is_duplicate(P1, 5)
    assert not d.is_duplicate(P1, 6)


def test_seq_zero_is_never_a_duplicate():
    d = Deduplicator()
    assert not d.is_duplicate(P1, 0)
    d.record(P1, 0)
    assert not d.is_duplicate(P1, 0)


def test_snapshot_restore_round_trip():
    d = Deduplicator()
    d.record(P1, 4)
    d.record(P2, 9)

    restored = Deduplicator()
    restored.restore(d.snapshot())

    assert restored.is_duplicate(P1, 4)
    assert restored.is_duplicate(P2, 9)
    assert not restored.is_duplicate(P1, 5)
