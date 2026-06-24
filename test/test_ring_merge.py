from typing import Callable
from uuid import uuid4

from ring_merge import MergeEofCounts, RingMerge

from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.messages import TransactionCount
from common.comms.middleware.mom import MOM


class _Rec:
    def __init__(self):
        self.events: list[str] = []


class _FakeCkpt:
    """Records flush() calls into the shared event log so the test can assert the
    checkpoint that freezes the spill happens BEFORE the emit sends."""

    def __init__(self, rec: _Rec):
        self._rec = rec

    def flush(self, force: bool = False):
        self._rec.events.append("flush")


class _RecTx(MOM):
    def __init__(self, rec: _Rec):
        self._rec = rec
        self.sent: list[bytes] = []

    def send(self, message: bytes):
        self._rec.events.append("send")
        self.sent.append(message)

    def start_consuming(self, on_message_callback: Callable):  # pragma: no cover
        raise NotImplementedError

    def stop_consuming(self):  # pragma: no cover
        raise NotImplementedError

    def close(self):  # pragma: no cover
        raise NotImplementedError

    def clone(self) -> "_RecTx":  # pragma: no cover
        return self


class _FakeMergeFn:
    def __init__(self, results):
        self._results = results

    def get_result(self, _client_id):
        yield from self._results

    def discard(self, _client_id):
        pass


def _node(fn, txs, checkpointer, counts):
    rc = RingCompletion(node_id=0, peer_ids=[])
    return RingMerge(
        fn=fn, node_id=0, rc=rc, counts=counts, consumer=None, ring=None,
        external_txs=txs, left_queue="l", left_exchange="l", right_queue="r",
        right_exchange="r", ring_queue="ring", ring_exchange="ring",
        data_prefetch=1, checkpointer=checkpointer,
    )


def test_checkpoint_is_flushed_before_emit_sends():
    cid = uuid4()
    rec = _Rec()
    results = [TransactionCount(cid, k) for k in range(3)]
    counts = MergeEofCounts()
    counts.left[cid] = 0
    counts.right[cid] = 0  # combined expected = 0, received = 0 -> Emit fires
    node = _node(_FakeMergeFn(results), [_RecTx(rec)], _FakeCkpt(rec), counts)

    node._maybe_complete(cid)

    assert "send" in rec.events, "the node should have emitted"
    # the spill-freezing flush must precede any emitted message, so a crash mid-emit
    # restores the frozen state and re-emits the same seqs (idempotent replay)
    assert rec.events.index("flush") < rec.events.index("send")
