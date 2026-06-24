from typing import Callable
from uuid import uuid4

from ring_aggregate import RingAggregate

from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.messages import EOF, TransactionCount, deserialize_message
from common.comms.middleware.mom import MOM
from common.comms.middleware.stamping_mom import StampingMOM, derive_producer_id


class _Cap(MOM):
    def __init__(self):
        self.sent: list[bytes] = []

    def send(self, message: bytes):
        self.sent.append(message)

    def start_consuming(self, on_message_callback: Callable):  # pragma: no cover
        raise NotImplementedError

    def stop_consuming(self):  # pragma: no cover
        raise NotImplementedError

    def close(self):  # pragma: no cover
        raise NotImplementedError

    def clone(self) -> "_Cap":  # pragma: no cover
        return self


class _FakeAggFn:
    """Yields a fixed result set, in whatever order the list is given — to simulate the
    divergent get_result order a node sees after a crash/restart replay."""

    def __init__(self, results):
        self._results = results  # list[(Message, affinity)]

    def get_result(self, _client_id):
        for msg, affinity in self._results:
            yield msg, affinity


def _txs(n):
    return [StampingMOM(_Cap(), derive_producer_id("agg", 0, route)) for route in range(n)]


def _node(fn, txs, broadcast):
    rc = RingCompletion(node_id=0, peer_ids=[])
    return RingAggregate(
        fn=fn, node_id=0, rc=rc, consumer=None, ring=None, external_txs=txs,
        data_queue="d", data_exchange="d", ring_queue="r", ring_exchange="r",
        data_prefetch=1, checkpointer=None, broadcast_downstream=broadcast,
    )


def _emit_and_collect(results, broadcast):
    """Run one _emit over `results` and return {payload_bytes: (producer_id, seq)} for the
    data messages it stamped (EOFs and other control messages are ignored)."""
    cid = uuid4()
    txs = _txs(2)
    node = _node(_FakeAggFn(results), txs, broadcast)
    node.rc.on_data(cid)
    node._emit(cid)
    stamps = {}
    for tx in txs:
        for raw in tx._inner.sent:  # type: ignore[attr-defined]
            msg = deserialize_message(raw)
            if isinstance(msg, EOF):
                continue
            # key each emitted result by (which shard's producer, the result's identity);
            # value is the (producer_id, seq) stamp it received
            stamps[(msg.producer_id, msg.count)] = (msg.producer_id, msg.seq)
    return stamps


def _results(_n):
    # distinct payloads (identified by count), spread across affinities so both shards
    # (affinity % 2) receive several results each
    return [(TransactionCount(uuid4(), k), k) for k in range(6)]


def test_sharded_emit_is_idempotent_across_replay_reorder():
    results = _results(6)
    first = _emit_and_collect(results, broadcast=False)
    second = _emit_and_collect(list(reversed(results)), broadcast=False)
    assert first == second
    assert len(first) == 6


def test_broadcast_emit_is_idempotent_across_replay_reorder():
    results = _results(6)
    first = _emit_and_collect(results, broadcast=True)
    second = _emit_and_collect(list(reversed(results)), broadcast=True)
    assert first == second
