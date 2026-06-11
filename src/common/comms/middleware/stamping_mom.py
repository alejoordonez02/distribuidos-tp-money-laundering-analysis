import hashlib
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

    def send(self, message: bytes):
        if peek_type(message) in _UNSTAMPED_TYPES:
            self._inner.send(message)
            return
        seq = self._counter.next()
        stamped = (
            message[: PREFIX_RANGE.stop]
            + self._producer_id
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


class UniqueStampingMOM(MOM):
    """Stamps each message with a producer_id unique to it (base id + counter),
    seq=1. At ingest into a competing stage this lets the watermark dedup work
    despite out-of-order delivery across peers (one producer + monotonic seq would
    be false-dropped on crash-redelivery)."""

    def __init__(
        self, inner: MOM, base_producer: bytes, counter: Optional[_SeqCounter] = None
    ):
        if len(base_producer) != _PRODUCER_LEN:
            raise ValueError(f"base_producer must be {_PRODUCER_LEN} bytes")
        self._inner = inner
        self._base = base_producer
        self._counter = counter if counter is not None else _SeqCounter()

    @property
    def producer_id(self) -> bytes:
        return self._base

    def seq_value(self) -> int:
        return self._counter.value()

    def restore_seq(self, value: int):
        self._counter.restore(value)

    def send(self, message: bytes):
        if peek_type(message) in _UNSTAMPED_TYPES:
            self._inner.send(message)
            return
        n = self._counter.next()
        producer_id = _derive_producer_id(self._base, n)
        stamped = (
            message[: PREFIX_RANGE.stop]
            + producer_id
            + (1).to_bytes(SEQ_BYTES, "big")
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

    def clone(self) -> "UniqueStampingMOM":
        return UniqueStampingMOM(self._inner.clone(), self._base, self._counter)


class InputContext:
    """Holds the identity of the message a competing-consumer node is currently
    processing, so its DerivedStampingMOMs can stamp outputs with an id derived
    from the input. Single-writer (the consuming thread sets it per message)."""

    def __init__(self):
        self._producer_id = b""
        self._seq = 0

    def set_input(self, producer_id: bytes, seq: int):
        self._producer_id = producer_id
        self._seq = seq

    def current(self) -> tuple[bytes, int]:
        return self._producer_id, self._seq


def _derive_producer_id(in_producer: bytes, in_seq: int) -> bytes:
    return hashlib.blake2b(
        in_producer + in_seq.to_bytes(SEQ_BYTES, "big"), digest_size=_PRODUCER_LEN
    ).digest()


class DerivedStampingMOM(MOM):
    """Stamps outputs of a competing-consumer node with an id DERIVED from the
    input being processed: producer_id = hash(input id), seq = a per-route index
    that restarts on each input. Whichever peer processes a given input emits the
    same ids, so a crash-redelivered duplicate is identical and gets deduplicated
    downstream — without coordinating producer ids across competing peers."""

    def __init__(self, inner: MOM, ctx: InputContext):
        self._inner = inner
        self._ctx = ctx
        self._last_input: tuple[bytes, int] = (b"", 0)
        self._sub = 0

    def send(self, message: bytes):
        if peek_type(message) in _UNSTAMPED_TYPES:
            self._inner.send(message)
            return
        in_id = self._ctx.current()
        if in_id != self._last_input:
            self._last_input = in_id
            self._sub = 0
        self._sub += 1
        producer_id = _derive_producer_id(in_id[0], in_id[1])
        stamped = (
            message[: PREFIX_RANGE.stop]
            + producer_id
            + self._sub.to_bytes(SEQ_BYTES, "big")
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

    def clone(self) -> "DerivedStampingMOM":
        # shared context; each clone keeps its own per-route sub-index
        return DerivedStampingMOM(self._inner.clone(), self._ctx)
