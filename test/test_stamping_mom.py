"""Phase 1: StampingMOM stamps data messages with producer_id + per-route seq."""

from typing import Callable
from uuid import uuid4

from common.comms.messages import (
    DEFAULT_PRODUCER,
    EOF,
    TransactionCount,
    deserialize_message,
)
from common.comms.middleware.mom import MOM
from common.comms.middleware.stamping_mom import StampingMOM, derive_producer_id


class _CapturingMOM(MOM):
    """Minimal MOM that records the bytes handed to send()."""

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

    def clone(self) -> "_CapturingMOM":
        return self  # share the buffer so clones record into the same place


def _data_msg() -> bytes:
    return TransactionCount(uuid4(), 1).serialize()


def test_data_messages_get_producer_and_monotonic_seq():
    inner = _CapturingMOM()
    producer = derive_producer_id("tx", 0, 0)
    tx = StampingMOM(inner, producer)

    tx.send(_data_msg())
    tx.send(_data_msg())
    tx.send(_data_msg())

    seqs = []
    for raw in inner.sent:
        msg = deserialize_message(raw)
        assert msg.producer_id == producer
        seqs.append(msg.seq)
    assert seqs == [1, 2, 3]


def test_eof_is_passed_through_unstamped():
    inner = _CapturingMOM()
    tx = StampingMOM(inner, derive_producer_id("tx", 0, 0))

    tx.send(EOF(uuid4(), processed_count=1).serialize())

    got = deserialize_message(inner.sent[0])
    assert isinstance(got, EOF)
    assert got.producer_id == DEFAULT_PRODUCER
    assert got.seq == 0


def test_eof_does_not_consume_seq_numbers():
    inner = _CapturingMOM()
    tx = StampingMOM(inner, derive_producer_id("tx", 0, 0))

    tx.send(_data_msg())
    tx.send(EOF(uuid4()).serialize())
    tx.send(_data_msg())

    data_seqs = [
        deserialize_message(r).seq
        for r in inner.sent
        if not isinstance(deserialize_message(r), EOF)
    ]
    assert data_seqs == [1, 2]


def test_different_routes_and_replicas_get_distinct_producers():
    p_route0 = derive_producer_id("tx", 0, 0)
    p_route1 = derive_producer_id("tx", 0, 1)
    p_replica1 = derive_producer_id("tx", 1, 0)
    p_other_tx = derive_producer_id("other_tx", 0, 0)

    assert len({p_route0, p_route1, p_replica1, p_other_tx}) == 4
    # Deterministic / stable across calls (a revived node keeps its identity).
    assert derive_producer_id("tx", 0, 0) == p_route0


def test_derive_producer_id_is_16_bytes():
    assert len(derive_producer_id("tx", 3, 2)) == 16


def test_clones_share_the_sequence():
    inner = _CapturingMOM()
    tx = StampingMOM(inner, derive_producer_id("tx", 0, 0))
    clone = tx.clone()

    tx.send(_data_msg())
    clone.send(_data_msg())
    tx.send(_data_msg())

    seqs = [deserialize_message(r).seq for r in inner.sent]
    assert seqs == [1, 2, 3]
