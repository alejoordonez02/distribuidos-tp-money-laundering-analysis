from typing import Any
from uuid import UUID


class SentCounts:
    """Per-client per-shard sent counts, snapshotable so they ride the checkpoint.

    A stateless node (filter / group-by) emits as it processes (not at EOF), so it
    must remember how many it has sent to each downstream shard to report an accurate
    per-shard count to the barrier at EOF. Counts only advance for unique (deduped)
    inputs, so they stay consistent with the downstream dedup after a crash.
    """

    def __init__(self):
        self._counts: dict[UUID, dict[int, int]] = {}

    def add(self, client_id: UUID, shard: int):
        self._counts.setdefault(client_id, {})[shard] = (
            self._counts.setdefault(client_id, {}).get(shard, 0) + 1
        )

    def pop(self, client_id: UUID) -> dict[int, int]:
        return self._counts.pop(client_id, {})

    def snapshot_state(self) -> dict[str, Any]:
        return {
            str(c): {str(s): n for s, n in shards.items()}
            for c, shards in self._counts.items()
        }

    def restore_state(self, snapshot: dict[str, Any]):
        self._counts = {
            UUID(c): {int(s): n for s, n in shards.items()}
            for c, shards in snapshot.items()
        }
