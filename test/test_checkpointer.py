from dataclasses import dataclass
from uuid import UUID, uuid4

from common.checkpoint.checkpoint_store import CheckpointStore
from common.checkpoint.checkpointer import Checkpointer
from common.checkpoint.deduplicator import Deduplicator

P1 = b"\x01" * 16


@dataclass
class FakeMsg:
    producer_id: bytes
    seq: int
    client_id: UUID = UUID(int=0)


class FakeFn:
    def __init__(self):
        self.applied: list[int] = []

    def snapshot_state(self):
        return {"applied": list(self.applied)}

    def restore_state(self, snapshot):
        self.applied = list(snapshot["applied"])


def _make(tmp_path, every):
    fn = FakeFn()
    store = CheckpointStore(str(tmp_path / "node.ckpt"))
    cp = Checkpointer(fn, store, Deduplicator(), every)
    return fn, store, cp


def test_acks_are_held_until_the_batch_is_full(tmp_path):
    fn, store, cp = _make(tmp_path, every=3)
    acked = []

    for seq in (1, 2):
        cp.handle_data(FakeMsg(P1, seq), lambda s=seq: fn.applied.append(s), lambda s=seq: acked.append(s))

    assert acked == []           # batch not full yet
    assert store.load() is None  # nothing checkpointed yet

    cp.handle_data(FakeMsg(P1, 3), lambda: fn.applied.append(3), lambda: acked.append(3))

    assert acked == [1, 2, 3]    # batch full -> flush
    assert store.load()["state"]["applied"] == [1, 2, 3]


def test_flush_checkpoints_and_acks_a_partial_batch(tmp_path):
    fn, store, cp = _make(tmp_path, every=10)
    acked = []
    cp.handle_data(FakeMsg(P1, 1), lambda: fn.applied.append(1), lambda: acked.append(1))

    cp.flush()

    assert acked == [1]
    assert store.load()["state"]["applied"] == [1]


def test_duplicate_is_discarded_and_acked_without_applying(tmp_path):
    fn, store, cp = _make(tmp_path, every=10)
    acked = []
    cp.handle_data(FakeMsg(P1, 1), lambda: fn.applied.append(1), lambda: acked.append("1"))
    cp.flush()

    cp.handle_data(FakeMsg(P1, 1), lambda: fn.applied.append(99), lambda: acked.append("dup"))

    assert fn.applied == [1]       # duplicate not applied
    assert "dup" in acked          # duplicate still acked


def test_restore_rebuilds_state_and_dedup(tmp_path):
    fn, store, cp = _make(tmp_path, every=1)
    cp.handle_data(FakeMsg(P1, 7), lambda: fn.applied.append(7), lambda: None)

    fn2 = FakeFn()
    cp2 = Checkpointer(fn2, store, Deduplicator(), 1)
    assert cp2.restore() is True

    assert fn2.applied == [7]
    # seq 7 is now known -> a redelivery of <= 7 is a duplicate
    discarded = {"v": False}
    cp2.handle_data(FakeMsg(P1, 7), lambda: fn2.applied.append(99), lambda: discarded.update(v=True))
    assert fn2.applied == [7]
    assert discarded["v"] is True


def test_restore_returns_false_when_no_checkpoint(tmp_path):
    _, _, cp = _make(tmp_path, every=1)
    assert cp.restore() is False


def test_aborted_client_data_is_dropped_and_acked(tmp_path):
    fn, _, cp = _make(tmp_path, every=10)
    cid = uuid4()
    cp.mark_aborted(cid)
    acked = {"v": False}

    cp.handle_data(
        FakeMsg(P1, 1, cid), lambda: fn.applied.append(1), lambda: acked.update(v=True)
    )

    assert cp.is_aborted(cid)
    assert fn.applied == []     # aborted client's data is not applied
    assert acked["v"] is True   # but still acked so it leaves the queue


def test_aborted_set_is_checkpointed_and_restored(tmp_path):
    fn, store, cp = _make(tmp_path, every=10)
    cid = uuid4()
    cp.mark_aborted(cid)
    cp.flush()

    cp2 = Checkpointer(FakeFn(), store, Deduplicator(), 10)
    assert cp2.restore() is True
    assert cp2.is_aborted(cid)


class FakeSeqSource:
    def __init__(self, producer_id: bytes, value: int = 0):
        self.producer_id = producer_id
        self._value = value

    def seq_value(self) -> int:
        return self._value

    def restore_seq(self, value: int):
        self._value = value


def test_output_seq_counter_is_checkpointed_and_restored(tmp_path):
    fn = FakeFn()
    store = CheckpointStore(str(tmp_path / "node.ckpt"))
    src = FakeSeqSource(P1, value=42)
    cp = Checkpointer(fn, store, Deduplicator(), 1, seq_sources=[src])

    cp.handle_data(FakeMsg(P1, 1), lambda: fn.applied.append(1), lambda: None)

    fn2 = FakeFn()
    restored_src = FakeSeqSource(P1, value=0)
    cp2 = Checkpointer(fn2, store, Deduplicator(), 1, seq_sources=[restored_src])
    cp2.restore()

    assert restored_src.seq_value() == 42
