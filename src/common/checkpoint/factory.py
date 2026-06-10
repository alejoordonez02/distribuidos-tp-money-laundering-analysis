import os
from typing import Mapping, Optional, Sequence

from .checkpoint_store import CheckpointStore
from .checkpointer import Checkpointer, SeqSource, Snapshotable
from .deduplicator import Deduplicator
from .null_state import NullState


def make_checkpointer(
    state_dir: Optional[str],
    node_id: str,
    seq_sources: Sequence[SeqSource],
    checkpoint_every: int,
    fn: Optional[Snapshotable] = None,
    extra_state: Optional[Mapping[str, Snapshotable]] = None,
) -> Optional[Checkpointer]:
    """Build a checkpointer for a controller, or None when checkpointing is off
    (no state dir). `fn` carries the business state; stateless nodes pass none and
    get a NullState (only dedup + output counters are persisted). `extra_state`
    persists extra namespaced state such as the EOF ring counters."""
    if not state_dir:
        return None
    store = CheckpointStore(os.path.join(state_dir, f"{node_id}.ckpt"))
    return Checkpointer(
        fn or NullState(),
        store,
        Deduplicator(),
        checkpoint_every,
        seq_sources,
        extra_state,
    )
