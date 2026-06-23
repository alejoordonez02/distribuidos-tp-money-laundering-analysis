from typing import Callable
from uuid import uuid4

from common.comms.messages import EOF, TransactionCount, deserialize_message
from common.comms.middleware.mom import MOM
from common.comms.middleware.stamping_mom import StampingMOM, derive_producer_id


class _CapturingMOM(MOM):
    def __init__(self):
        self.sent: list[bytes] = []

    def send(self, message: bytes, routing_key: str | None = None):
        self.sent.append(message)

    def start_consuming(self, on_message_callback: Callable):  # pragma: no cover
        raise NotImplementedError

    def stop_consuming(self):  # pragma: no cover
        raise NotImplementedError

    def close(self):  # pragma: no cover
        raise NotImplementedError

    def clone(self) -> "_CapturingMOM":
        return self


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
    assert derive_producer_id("tx", 0, 0) == p_route0


def test_seq_value_and_restore_seq():
    inner = _CapturingMOM()
    tx = StampingMOM(inner, derive_producer_id("tx", 0, 0))

    tx.send(_data_msg())
    tx.send(_data_msg())
    assert tx.seq_value() == 2

    tx.restore_seq(100)
    tx.send(_data_msg())
    assert deserialize_message(inner.sent[-1]).seq == 101
