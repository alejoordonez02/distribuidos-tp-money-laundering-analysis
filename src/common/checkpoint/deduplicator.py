from typing import Optional


class Deduplicator:
    """Tracks the highest seq seen per producer, so redelivered messages already
    covered by a checkpoint can be discarded without storing every seen id."""

    def __init__(self, last_seq: Optional[dict[bytes, int]] = None):
        self._last_seq: dict[bytes, int] = dict(last_seq) if last_seq else {}

    def is_duplicate(self, producer_id: bytes, seq: int) -> bool:
        if seq == 0:
            # Unstamped message (no producer identity): cannot be deduplicated.
            return False
        return seq <= self._last_seq.get(producer_id, 0)

    def record(self, producer_id: bytes, seq: int):
        if seq > self._last_seq.get(producer_id, 0):
            self._last_seq[producer_id] = seq

    def snapshot(self) -> dict[bytes, int]:
        return dict(self._last_seq)

    def restore(self, snapshot: dict[bytes, int]):
        self._last_seq = dict(snapshot)
