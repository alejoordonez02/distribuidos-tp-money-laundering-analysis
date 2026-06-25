from uuid import uuid4

from ring_aggregate import RingAggregate

from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.messages import EOF, TransactionCount

from ._fakes import _FakeCkpt, _Rec, _RecTx


class _FakeAggFn:
    def __init__(self, results):
        self._results = results

    def get_result(self, _client_id):
        for msg, affinity in self._results:
            yield msg, affinity

    def discard(self, _client_id):
        pass


def _node(fn, txs, ckpt, broadcast):
    rc = RingCompletion(node_id=0, peer_ids=[])
    return RingAggregate(
        fn=fn, node_id=0, rc=rc, consumer=None, ring=None, external_txs=txs,
        data_queue="d", data_exchange="d", ring_queue="r", ring_exchange="r",
        data_prefetch=1, checkpointer=ckpt, broadcast_downstream=broadcast,
    )


def test_checkpoint_is_flushed_before_emit_sends():
    cid = uuid4()
    rec = _Rec()
    results = [(TransactionCount(cid, k), k) for k in range(4)]
    node = _node(_FakeAggFn(results), [_RecTx(rec), _RecTx(rec)], _FakeCkpt(rec), False)

    node._on_eof(EOF(cid, expected_count=0))

    assert "send" in rec.events
    assert rec.events.index("flush") < rec.events.index("send")


def test_streaming_emit_does_not_materialize_results():
    cid = uuid4()
    rec = _Rec()
    txs = [_RecTx(rec)]
    sends = []

    class _Counting:
        def get_result(self, _c):
            for k in range(5):
                sends.append(len(txs[0].sent))
                yield TransactionCount(cid, k), 0

        def discard(self, _c):
            pass

    node = _node(_Counting(), txs, _FakeCkpt(rec), False)
    node._on_eof(EOF(cid, expected_count=0))

    assert txs[0].sent and sends == [0, 1, 2, 3, 4]
