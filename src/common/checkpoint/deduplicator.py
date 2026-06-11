from collections import OrderedDict
from typing import Optional

# LRU cap: must exceed the crash-redelivery gap so a stale duplicate cannot slip through
MAX_PRODUCERS = 100_000


class Deduplicator:
    """Tracks the highest seq seen per producer, so redelivered messages already
    covered by a checkpoint can be discarded without storing every seen id. The
    table is LRU-bounded so derived-id (competing-consumer) streams stay capped."""

    def __init__(
        self,
        last_seq: Optional[dict[bytes, int]] = None,
        max_producers: int = MAX_PRODUCERS,
    ):
        self._max = max_producers
        self._last_seq: "OrderedDict[bytes, int]" = OrderedDict(last_seq or {})

    def is_duplicate(self, producer_id: bytes, seq: int) -> bool:
        if seq == 0:
            # Unstamped message (no producer identity): cannot be deduplicated.
            return False
        return seq <= self._last_seq.get(producer_id, 0)

    def record(self, producer_id: bytes, seq: int):
        if seq <= self._last_seq.get(producer_id, 0):
            return
        self._last_seq[producer_id] = seq
        self._last_seq.move_to_end(producer_id)
        if len(self._last_seq) > self._max:
            self._last_seq.popitem(last=False)

    def snapshot(self) -> dict[bytes, int]:
        return dict(self._last_seq)

    def restore(self, snapshot: dict[bytes, int]):
        self._last_seq = OrderedDict(snapshot)
