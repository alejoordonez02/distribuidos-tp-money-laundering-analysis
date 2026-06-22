from common.checkpoint.deduplicator import Deduplicator

P1 = b"\x01" * 16
P2 = b"\x02" * 16


def test_first_seq_is_not_duplicate():
    d = Deduplicator()
    assert not d.is_duplicate(P1, 1)


def test_seen_seq_is_a_duplicate():
    d = Deduplicator()
    d.record(P1, 5)
    assert d.is_duplicate(P1, 5)
    assert not d.is_duplicate(P1, 6)


def test_unseen_seq_below_a_higher_one_is_not_a_duplicate():
    d = Deduplicator()
    d.record(P1, 5)
    assert not d.is_duplicate(P1, 3)
    assert not d.is_duplicate(P1, 4)


def test_producers_are_independent():
    d = Deduplicator()
    d.record(P1, 7)
    assert d.is_duplicate(P1, 7)
    assert not d.is_duplicate(P2, 7)


def test_contiguous_records_advance_the_watermark():
    d = Deduplicator()
    for s in (1, 2, 3):
        d.record(P1, s)
    assert d.snapshot()[P1] == [3, []]
    assert d.is_duplicate(P1, 2)
    assert not d.is_duplicate(P1, 4)


def test_out_of_order_records_keep_the_gap_until_filled():
    d = Deduplicator()
    d.record(P1, 3)
    d.record(P1, 1)
    assert d.is_duplicate(P1, 1)
    assert d.is_duplicate(P1, 3)
    assert not d.is_duplicate(P1, 2)
    d.record(P1, 2)
    assert d.is_duplicate(P1, 2)
    assert d.snapshot()[P1] == [3, []]


def test_recording_a_seq_twice_is_idempotent():
    d = Deduplicator()
    d.record(P1, 5)
    d.record(P1, 5)
    assert d.snapshot()[P1] == [0, [5]]


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
    assert not restored.is_duplicate(P1, 3)
