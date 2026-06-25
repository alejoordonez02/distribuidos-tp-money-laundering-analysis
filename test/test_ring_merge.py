from uuid import uuid4

from ring_merge import MergeEofCounts, RingMerge

from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.messages import TransactionCount

from ._fakes import _FakeCkpt, _Rec, _RecTx


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
    # the spill-freezing flush must precede any emit, so a crash mid-emit restores the frozen state and re-emits the same seqs (idempotent replay)
    assert rec.events.index("flush") < rec.events.index("send")
