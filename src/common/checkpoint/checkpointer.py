from threading import Lock
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence

from common.fault_injection import maybe_crash

from .checkpoint_store import CheckpointStore
from .deduplicator import Deduplicator


class Snapshotable(Protocol):
    def snapshot_state(self) -> dict[str, Any]: ...
    def restore_state(self, snapshot: dict[str, Any]): ...


class SeqSource(Protocol):
    producer_id: bytes

    def seq_value(self) -> int: ...
    def restore_seq(self, value: int): ...


class Checkpointer:
    """Coordinates dedup + batched checkpointing for a stateful node. Holds ACKs
    until the checkpoint that covers them is durable, so a crash never loses an
    acknowledged effect. `checkpoint_every` controls the batch (and ACK) cadence.

    Thread-safe: `lock` guards the business state, so a concurrent reader (EOF
    result emission) must take it around snapshot-sensitive access.
    """

    def __init__(
        self,
        fn: Snapshotable,
        store: CheckpointStore,
        deduplicator: Deduplicator,
        checkpoint_every: int,
        seq_sources: Sequence[SeqSource] = (),
        extra_state: Optional[Mapping[str, Snapshotable]] = None,
    ):
        self._fn = fn
        self._store = store
        self._dedup = deduplicator
        self._every = max(1, checkpoint_every)
        self._seq_sources = seq_sources
        # Extra namespaced state providers (e.g. the EOF ring counters), persisted
        # atomically with the business state and dedup table.
        self._extra_state = dict(extra_state or {})
        self._pending_acks: list[Callable] = []
        self._dirty = False
        self.lock = Lock()

    def restore(self) -> bool:
        blob = self._store.load()
        if blob is None:
            return False
        self._fn.restore_state(blob["state"])
        self._dedup.restore(blob["dedup"])
        out_seq = blob.get("out_seq", {})
        for src in self._seq_sources:
            if src.producer_id in out_seq:
                src.restore_seq(out_seq[src.producer_id])
        extra = blob.get("extra", {})
        for key, provider in self._extra_state.items():
            if key in extra:
                provider.restore_state(extra[key])
        maybe_crash("after_restore_on_startup")
        return True

    def handle_data(self, msg, apply: Callable, ack: Callable):
        with self.lock:
            if self._dedup.is_duplicate(msg.producer_id, msg.seq):
                maybe_crash("after_dup_before_ack")
                ack()
                return

            apply()
            self._dedup.record(msg.producer_id, msg.seq)
            self._dirty = True
            self._pending_acks.append(ack)
            maybe_crash("after_apply_before_checkpoint")

            if len(self._pending_acks) >= self._every:
                self._checkpoint_and_flush()

    def flush(self):
        with self.lock:
            self._checkpoint_and_flush()

    def _checkpoint_and_flush(self):
        if self._dirty:
            self._store.save(
                {
                    "state": self._fn.snapshot_state(),
                    "dedup": self._dedup.snapshot(),
                    "out_seq": {s.producer_id: s.seq_value() for s in self._seq_sources},
                    "extra": {
                        k: p.snapshot_state() for k, p in self._extra_state.items()
                    },
                }
            )
            self._dirty = False

        maybe_crash("after_checkpoint_before_ack")

        for ack in self._pending_acks:
            ack()
        self._pending_acks.clear()
