import glob
import os
from typing import Any, IO, Iterator
from uuid import UUID


class MultiShardSpill:
    """Per-(client, shard) append-only files on a durable dir, with crash-consistent
    snapshot/restore.

    Like PersistentSpill but keyed by (client, shard): the heavy UC4 aggregates
    bucket their state into ~hundreds of shard files. The checkpoint records each
    file's committed byte length; `restore_state` truncates anything appended after
    the last checkpoint (re-appended when un-acked messages are redelivered).
    """

    def __init__(self, directory: str, tag: str):
        self._dir = directory
        self._tag = tag
        self._handles: dict[tuple[UUID, int], IO[str]] = {}
        os.makedirs(directory, exist_ok=True)

    def _path(self, client_id: UUID, shard: int) -> str:
        return os.path.join(self._dir, f"{self._tag}_{client_id}_{shard}.spill")

    def append(self, client_id: UUID, shard: int, line: str):
        key = (client_id, shard)
        handle = self._handles.get(key)
        if handle is None:
            handle = open(self._path(client_id, shard), "a")
            self._handles[key] = handle
        handle.write(line)

    def _disk_shards(self, client_id: UUID) -> set[int]:
        prefix = os.path.join(self._dir, f"{self._tag}_{client_id}_")
        shards = set()
        for path in glob.glob(f"{prefix}*.spill"):
            try:
                shards.add(int(path[len(prefix) : -len(".spill")]))
            except ValueError:
                pass
        return shards

    def shards_of(self, client_id: UUID) -> list[int]:
        from_handles = {s for (c, s) in self._handles if c == client_id}
        return sorted(from_handles | self._disk_shards(client_id))

    def read_shard(self, client_id: UUID, shard: int) -> Iterator[str]:
        handle = self._handles.pop((client_id, shard), None)
        if handle is not None:
            handle.close()
        path = self._path(client_id, shard)
        if os.path.exists(path):
            with open(path) as f:
                yield from f

    def clear(self, client_id: UUID):
        for key in [k for k in self._handles if k[0] == client_id]:
            self._handles.pop(key).close()
        for shard in self._disk_shards(client_id):
            try:
                os.unlink(self._path(client_id, shard))
            except OSError:
                pass

    def clear_all(self):
        for handle in self._handles.values():
            handle.close()
        self._handles = {}
        for path in glob.glob(os.path.join(self._dir, f"{self._tag}_*.spill")):
            try:
                os.unlink(path)
            except OSError:
                pass

    def snapshot_state(self) -> dict[str, Any]:
        # flush() pushes the buffer to the OS; no fsync — the fault model is a
        # process crash (RabbitMQ stable), and OS page cache survives that. fsync
        # (for power loss) is out of scope and would cost hundreds of syncs/ckpt.
        committed = {}
        for (client_id, shard), handle in self._handles.items():
            handle.flush()
            committed[f"{client_id}|{shard}"] = os.path.getsize(
                self._path(client_id, shard)
            )
        return committed

    def restore_state(self, snapshot: dict[str, Any]):
        self._handles = {}
        for key, length in snapshot.items():
            client_str, shard_str = key.rsplit("|", 1)
            path = self._path(UUID(client_str), int(shard_str))
            if os.path.exists(path):
                with open(path, "r+") as f:
                    f.truncate(length)
