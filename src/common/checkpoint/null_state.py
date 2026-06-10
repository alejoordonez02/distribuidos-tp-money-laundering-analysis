from typing import Any


class NullState:
    """Snapshotable for stateless nodes: no business state, so only the dedup table
    and output sequence counters get checkpointed."""

    def snapshot_state(self) -> dict[str, Any]:
        return {}

    def restore_state(self, snapshot: dict[str, Any]):
        pass
