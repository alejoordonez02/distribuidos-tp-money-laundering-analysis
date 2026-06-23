import uuid
from threading import Lock
from typing import Callable, Optional

from common.comms.messages import (
    PREFIX_RANGE,
    PRODUCER_RANGE,
    SEQ_BYTES,
    SEQ_RANGE,
    MessageType,
    peek_type,
)

from .mom import MOM

_PRODUCER_NAMESPACE = uuid.UUID("6f1c5e1e-0000-4000-8000-000000000001")
_PRODUCER_LEN = PRODUCER_RANGE.stop - PRODUCER_RANGE.start

# Control/handshake messages carry no identity: skipping them keeps the EOF ring
# bytes untouched and avoids advancing the per-route sequence.
_UNSTAMPED_TYPES = frozenset(
    int(t)
    for t in (
        MessageType.EOF,
        MessageType.ABORT,
        MessageType.RING_DONE,
        MessageType.RING_SENT_DATA,
        MessageType.FIN,
        MessageType.HELLO,
        MessageType.HELLO_ACK,
    )
)


def derive_producer_id(tx_name: str, idx: int, route: int) -> bytes:
    """Stable 16-byte id for a (node-instance, output-route), unchanged across
    restarts and unique per replica and route."""
    return uuid.uuid5(_PRODUCER_NAMESPACE, f"{tx_name}:{idx}:{route}").bytes


class _SeqCounter:
    def __init__(self):
        self._seq = 0
        self._lock = Lock()

    def next(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    def value(self) -> int:
        with self._lock:
            return self._seq

    def restore(self, value: int):
        with self._lock:
            self._seq = value


# Public alias: a sequence counter that can be shared between several StampingMOMs
# (e.g. a node whose output is emitted from more than one thread/connection).
SeqCounter = _SeqCounter


class CounterSeqSource:
    """A SeqSource view over a shared counter, so it can be checkpointed without
    holding a connection (used when the real stampers are created per-thread)."""

    def __init__(self, producer_id: bytes, counter: _SeqCounter):
        self.producer_id = producer_id
        self._counter = counter

    def seq_value(self) -> int:
        return self._counter.value()

    def restore_seq(self, value: int):
        self._counter.restore(value)


class StampingMOM(MOM):
    """Decorates a tx MOM, stamping each data message with a `producer_id` and a
    per-route incrementing `seq` by rewriting the header bytes."""

    def __init__(
        self, inner: MOM, producer_id: bytes, counter: Optional[_SeqCounter] = None
    ):
        if len(producer_id) != _PRODUCER_LEN:
            raise ValueError(f"producer_id must be {_PRODUCER_LEN} bytes")
        self._inner = inner
        self._producer_id = producer_id
        self._counter = counter if counter is not None else _SeqCounter()

    @property
    def producer_id(self) -> bytes:
        return self._producer_id

    def seq_value(self) -> int:
        return self._counter.value()

    def restore_seq(self, value: int):
        # Resume the sequence after a restart so re-emitted messages keep the same
        # seq and stay deduplicable downstream.
        self._counter.restore(value)

    def send(self, message: bytes, routing_key: str | None = None):
        if peek_type(message) in _UNSTAMPED_TYPES:
            self._inner.send(message, routing_key)
            return
        seq = self._counter.next()
        stamped = (
            message[: PREFIX_RANGE.stop]
            + self._producer_id
            + seq.to_bytes(SEQ_BYTES, "big")
            + message[SEQ_RANGE.stop :]
        )
        self._inner.send(stamped, routing_key)

    def send_stamped(self, message: bytes, producer_id: bytes, seq: int):
        if peek_type(message) in _UNSTAMPED_TYPES:
            self._inner.send(message)
            return
        stamped = (
            message[: PREFIX_RANGE.stop]
            + producer_id
            + seq.to_bytes(SEQ_BYTES, "big")
            + message[SEQ_RANGE.stop :]
        )
        self._inner.send(stamped)

    def start_consuming(
        self, on_message_callback: Callable[[bytes, Callable, Callable], None]
    ):
        self._inner.start_consuming(on_message_callback)

    def stop_consuming(self):
        self._inner.stop_consuming()

    def close(self):
        self._inner.close()

    def clone(self) -> "StampingMOM":
        # Share the counter so cloned handles to the same route keep one sequence.
        return StampingMOM(self._inner.clone(), self._producer_id, self._counter)
