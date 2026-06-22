from typing import Optional


class Deduplicator:
    """Per-producer exactly-once filter that is safe under reordered redelivery: it keeps
    the highest contiguous seq seen (`hi`) plus a set of seqs seen ahead of a gap, so a
    redelivered message is dropped only if that exact seq was already processed — never a
    never-seen seq just because it is below the highest. All ops are O(1). For an in-order
    stream `hi` advances and the ahead-set stays empty; only out-of-order gaps cost memory,
    bounded by the in-flight (un-acked) window."""

    def __init__(self, state: Optional[dict] = None):
        self._hi: dict[bytes, int] = {}
        self._ahead: dict[bytes, set[int]] = {}
        for producer, (hi, ahead) in (state or {}).items():
            self._hi[producer] = hi
            self._ahead[producer] = set(ahead)

    def is_duplicate(self, producer_id: bytes, seq: int) -> bool:
        if seq == 0:
            return False
        if seq <= self._hi.get(producer_id, 0):
            return True
        return seq in self._ahead.get(producer_id, ())

    def record(self, producer_id: bytes, seq: int):
        if seq == 0:
            return
        hi = self._hi.get(producer_id, 0)
        if seq <= hi:
            return
        ahead = self._ahead.setdefault(producer_id, set())
        ahead.add(seq)
        while hi + 1 in ahead:
            hi += 1
            ahead.discard(hi)
        self._hi[producer_id] = hi

    def snapshot(self) -> dict[bytes, list]:
        return {
            producer: [self._hi.get(producer, 0), sorted(self._ahead.get(producer, ()))]
            for producer in set(self._hi) | set(self._ahead)
        }

    def restore(self, snapshot: dict[bytes, list]):
        self._hi = {}
        self._ahead = {}
        for producer, (hi, ahead) in snapshot.items():
            self._hi[producer] = hi
            self._ahead[producer] = set(ahead)
