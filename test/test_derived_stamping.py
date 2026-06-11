"""Competing-consumer crash safety: DerivedStampingMOM stamps outputs with an id
derived from the input, so whichever peer processes a given input emits identical
ids and a crash-redelivered duplicate is caught by the downstream deduplicator."""

from typing import Callable
from uuid import uuid4

from common.checkpoint.deduplicator import Deduplicator
from common.comms.messages import (
    DEFAULT_PRODUCER,
    EOF,
    TransactionCount,
    deserialize_message,
)
from common.comms.middleware.mom import MOM
from common.comms.middleware.stamping_mom import (
    DerivedStampingMOM,
    InputContext,
    derive_producer_id,
)


class _CapturingMOM(MOM):
    def __init__(self):
        self.sent: list[bytes] = []

    def send(self, message: bytes):
        self.sent.append(message)

    def start_consuming(self, cb: Callable):  # pragma: no cover
        raise NotImplementedError

    def stop_consuming(self):  # pragma: no cover
        raise NotImplementedError

    def close(self):  # pragma: no cover
        raise NotImplementedError

    def clone(self) -> "_CapturingMOM":
        return self


def _peer():
    inner = _CapturingMOM()
    ctx = InputContext()
    return inner, ctx, DerivedStampingMOM(inner, ctx)


def _process_input(ctx, tx, in_producer, in_seq, n_outputs):
    ctx.set_input(in_producer, in_seq)
    for _ in range(n_outputs):
        tx.send(TransactionCount(uuid4(), 1).serialize())


def test_same_input_yields_same_ids_on_any_peer():
    in_prod, in_seq = derive_producer_id("upstream", 0, 0), 42

    a_inner, a_ctx, a_tx = _peer()
    b_inner, b_ctx, b_tx = _peer()

    _process_input(a_ctx, a_tx, in_prod, in_seq, 3)
    _process_input(b_ctx, b_tx, in_prod, in_seq, 3)

    a_ids = [(deserialize_message(r).producer_id, deserialize_message(r).seq) for r in a_inner.sent]
    b_ids = [(deserialize_message(r).producer_id, deserialize_message(r).seq) for r in b_inner.sent]
    assert a_ids == b_ids
    assert [s for _, s in a_ids] == [1, 2, 3]
    assert len({p for p, _ in a_ids}) == 1  # one derived producer for the input


def test_sub_index_restarts_per_input():
    inner, ctx, tx = _peer()
    _process_input(ctx, tx, derive_producer_id("u", 0, 0), 1, 2)
    _process_input(ctx, tx, derive_producer_id("u", 0, 0), 2, 2)

    msgs = [deserialize_message(r) for r in inner.sent]
    assert [m.seq for m in msgs] == [1, 2, 1, 2]
    # the two inputs derive distinct producer ids
    assert msgs[0].producer_id != msgs[2].producer_id


def test_crash_duplicate_is_caught_by_hwm_downstream():
    in_prod, in_seq = derive_producer_id("u", 0, 0), 7
    dedup = Deduplicator()

    # peer A processes the input, emits, then "crashes" (its output is downstream)
    a_inner, a_ctx, a_tx = _peer()
    _process_input(a_ctx, a_tx, in_prod, in_seq, 3)
    for r in a_inner.sent:
        m = deserialize_message(r)
        assert not dedup.is_duplicate(m.producer_id, m.seq)
        dedup.record(m.producer_id, m.seq)

    # the input is redelivered to peer B, which re-emits it: all duplicates
    b_inner, b_ctx, b_tx = _peer()
    _process_input(b_ctx, b_tx, in_prod, in_seq, 3)
    for r in b_inner.sent:
        m = deserialize_message(r)
        assert dedup.is_duplicate(m.producer_id, m.seq)


def test_partial_crash_keeps_each_output_once():
    in_prod, in_seq = derive_producer_id("u", 0, 0), 9
    dedup = Deduplicator()
    accepted = 0

    # A emits only 2 of 3 before crashing
    a_inner, a_ctx, a_tx = _peer()
    _process_input(a_ctx, a_tx, in_prod, in_seq, 2)
    for r in a_inner.sent:
        m = deserialize_message(r)
        if not dedup.is_duplicate(m.producer_id, m.seq):
            dedup.record(m.producer_id, m.seq)
            accepted += 1

    # B re-emits all 3: the first 2 dedup, the 3rd is new
    b_inner, b_ctx, b_tx = _peer()
    _process_input(b_ctx, b_tx, in_prod, in_seq, 3)
    for r in b_inner.sent:
        m = deserialize_message(r)
        if not dedup.is_duplicate(m.producer_id, m.seq):
            dedup.record(m.producer_id, m.seq)
            accepted += 1

    assert accepted == 3


def test_eof_passes_through_unstamped():
    inner, ctx, tx = _peer()
    ctx.set_input(derive_producer_id("u", 0, 0), 1)
    tx.send(EOF(uuid4(), processed_count=1).serialize())

    got = deserialize_message(inner.sent[0])
    assert isinstance(got, EOF)
    assert got.producer_id == DEFAULT_PRODUCER
    assert got.seq == 0
