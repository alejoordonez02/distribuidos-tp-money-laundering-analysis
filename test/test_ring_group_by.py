from types import SimpleNamespace
from typing import Callable
from uuid import uuid4

from ring_group_by import RingGroupBy

from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.messages import TransactionCount, deserialize_message
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


class _FakeGroupByFn:
    """Deterministic 1:N grouping: every input yields the same (group, affinity) list,
    so re-processing a redelivered input must reproduce identical output identities."""

    def __init__(self, groups):
        self._groups = groups

    def group_by(self, _msg):
        return list(self._groups)


def _stamp_txs(n):
    return [StampingMOM(_Cap(), derive_producer_id("gb", 0, route)) for route in range(n)]


def _node(fn, fleets):
    rc = RingCompletion(node_id=0, peer_ids=[])
    flat = [tx for fleet in fleets for tx in fleet]
    return RingGroupBy(
        fn=fn, node_id=0, rc=rc, sent=SentCounts(), consumer=None, ring=None,
        fleets=fleets, data_queue="d", data_exchange="d", ring_queue="r",
        ring_exchange="r", data_prefetch=1, checkpointer=None,
    )


def _stamps(fleets):
    out = set()
    for fleet in fleets:
        for tx in fleet:
            for raw in tx._inner.sent:  # type: ignore[attr-defined]
                m = deserialize_message(raw)
                out.add((m.producer_id, m.seq, m.count))
    return out


def _clear(fleets):
    for fleet in fleets:
        for tx in fleet:
            tx._inner.sent.clear()  # type: ignore[attr-defined]


def test_reprocessing_a_redelivered_input_is_idempotent():
    cid = uuid4()
    # 4 distinct groups spread across affinities so several land on the same shard
    groups = [(TransactionCount(cid, k), k) for k in range(4)]
    fleets = [_stamp_txs(2)]
    node = _node(_FakeGroupByFn(groups), fleets)
    msg = SimpleNamespace(client_id=cid, seq=7, producer_id=derive_producer_id("up", 0, 0))

    node._on_data(msg)
    first = _stamps(fleets)
    _clear(fleets)
    node._on_data(msg)  # the same input, redelivered after a restart
    second = _stamps(fleets)

    assert first == second
    assert len(first) == 4  # the 4 groups all carried distinct (producer, seq) identities


def test_same_seq_from_distinct_upstream_producers_do_not_collide():
    cid = uuid4()
    groups = [(TransactionCount(cid, k), k) for k in range(4)]
    fleets = [_stamp_txs(2)]
    node = _node(_FakeGroupByFn(groups), fleets)
    # two different upstream producers happen to reuse the same seq value
    a = SimpleNamespace(client_id=cid, seq=5, producer_id=derive_producer_id("upA", 0, 0))
    b = SimpleNamespace(client_id=cid, seq=5, producer_id=derive_producer_id("upB", 0, 0))

    node._on_data(a)
    node._on_data(b)

    # 8 outputs (4 groups x 2 inputs), all with distinct (producer, seq) — no collision
    assert len(_stamps(fleets)) == 8
