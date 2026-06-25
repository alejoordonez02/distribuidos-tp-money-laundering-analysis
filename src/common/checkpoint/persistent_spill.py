import glob
import os
from typing import IO, Any, Iterator
from uuid import UUID


class PersistentSpill:
    """Per-client append-only line buffer on a durable directory.

    Survives a restart: the checkpoint records each file's committed byte length,
    and `restore_state` truncates anything appended after the last checkpoint (those
    lines get re-appended when the un-acked messages are redelivered), so the spill
    stays consistent with the dedup table.
    """

    def __init__(self, directory: str, tag: str):
        self._dir = directory
        self._tag = tag
        self._handles: dict[UUID, IO[str]] = {}
        os.makedirs(directory, exist_ok=True)

    def _path(self, client_id: UUID) -> str:
        return os.path.join(self._dir, f"{self._tag}_{client_id}.spill")

    def _open_append(self, path: str) -> IO[str]:
        try:
            return open(path, "a")
        except FileNotFoundError:
            os.makedirs(self._dir, exist_ok=True)
            return open(path, "a")

    def _rmdir_if_empty(self):
        try:
            os.rmdir(self._dir)
        except OSError:
            pass

    def _handle(self, client_id: UUID) -> IO[str]:
        handle = self._handles.get(client_id)
        if handle is None:
            handle = self._open_append(self._path(client_id))
            self._handles[client_id] = handle
        return handle

    def append(self, client_id: UUID, line: str):
        self._handle(client_id).write(line)

    def iter_lines(self, client_id: UUID) -> Iterator[str]:
        handle = self._handles.get(client_id)
        if handle is not None:
            handle.flush()
        path = self._path(client_id)
        if os.path.exists(path):
            with open(path) as f:
                yield from f

    def iter_chunks(self, client_id: UUID, batch_lines: int) -> Iterator[str]:
        batch: list[str] = []
        for line in self.iter_lines(client_id):
            batch.append(line)
            if len(batch) >= batch_lines:
                yield "".join(batch)
                batch = []
        if batch:
            yield "".join(batch)

    def iter_lines_and_clear(self, client_id: UUID) -> Iterator[str]:
        handle = self._handles.pop(client_id, None)
        if handle is not None:
            handle.close()
        path = self._path(client_id)
        if os.path.exists(path):
            with open(path) as f:
                yield from f
            os.unlink(path)
            self._rmdir_if_empty()

    def clear(self, client_id: UUID):
        """Drop a client's spilled lines without reading them (used on abort)."""
        handle = self._handles.pop(client_id, None)
        if handle is not None:
            handle.close()
        path = self._path(client_id)
        if os.path.exists(path):
            os.unlink(path)
            self._rmdir_if_empty()

    def clear_all(self):
        for handle in self._handles.values():
            handle.close()
        self._handles = {}
        for path in glob.glob(os.path.join(self._dir, f"{self._tag}_*.spill")):
            try:
                os.unlink(path)
            except OSError:
                pass
        self._rmdir_if_empty()

    def iter_chunks_and_clear(
        self, client_id: UUID, batch_lines: int
    ) -> Iterator[str]:
        """Yield the spilled lines joined in batches of `batch_lines`, then delete
        the file (satisfies the join's Spill protocol)."""
        batch: list[str] = []
        for line in self.iter_lines_and_clear(client_id):
            batch.append(line)
            if len(batch) >= batch_lines:
                yield "".join(batch)
                batch = []
        if batch:
            yield "".join(batch)

    def snapshot_state(self) -> dict[str, Any]:
        committed = {}
        for client_id, handle in self._handles.items():
            handle.flush()  # no fsync: process-crash model, OS page cache survives
            committed[str(client_id)] = os.path.getsize(self._path(client_id))
        return committed

    def restore_state(self, snapshot: dict[str, Any]):
        self._handles = {}
        for client_str, length in snapshot.items():
            client_id = UUID(client_str)
            path = self._path(client_id)
            if os.path.exists(path):
                with open(path, "r+") as f:
                    f.truncate(length)
            self._handles[client_id] = self._open_append(path)
