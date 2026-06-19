import os
import tempfile
from typing import Iterator, Protocol
from uuid import UUID

from common.comms.messages import Response


class Spill(Protocol):
    def append(self, client_id: UUID, line: str) -> None: ...

    def iter_chunks_and_clear(
        self, client_id: UUID, batch_lines: int
    ) -> Iterator[str]: ...

    def clear(self, client_id: UUID) -> None: ...


class LineSpill:
    """Append-only on-disk buffer of response lines, keyed by client.

    Keeps RAM O(1) by writing each line to a per-client temp file and replaying
    it at EOF, instead of holding millions of Transaction objects in memory.
    """

    def __init__(self, tag: str):
        self._tag = tag
        self._handles: dict[UUID, object] = {}
        self._paths: dict[UUID, str] = {}

    def append(self, client_id: UUID, line: str) -> None:
        handle = self._handles.get(client_id)
        if handle is None:
            fd, path = tempfile.mkstemp(prefix=f"{self._tag}:{client_id}:", suffix=".txt")
            handle = os.fdopen(fd, "w")
            self._handles[client_id] = handle
            self._paths[client_id] = path
        handle.write(line)  # type: ignore[attr-defined]

    def clear(self, client_id: UUID) -> None:
        handle = self._handles.pop(client_id, None)
        path = self._paths.pop(client_id, None)
        if handle is not None:
            handle.close()  # type: ignore[attr-defined]
        if path is not None and os.path.exists(path):
            os.unlink(path)

    def read_and_clear(self, client_id: UUID) -> str:
        handle = self._handles.pop(client_id, None)
        path = self._paths.pop(client_id, None)
        if handle is None or path is None:
            return ""
        handle.close()  # type: ignore[attr-defined]
        try:
            with open(path, "r") as f:
                return f.read()
        finally:
            os.unlink(path)

    def iter_chunks_and_clear(
        self, client_id: UUID, batch_lines: int
    ) -> Iterator[str]:
        """Yield the spilled lines in batches of `batch_lines`, then delete the file."""
        handle = self._handles.pop(client_id, None)
        path = self._paths.pop(client_id, None)
        if handle is None or path is None:
            return
        handle.close()  # type: ignore[attr-defined]
        try:
            with open(path, "r") as f:
                batch: list[str] = []
                for line in f:
                    batch.append(line)
                    if len(batch) >= batch_lines:
                        yield "".join(batch)
                        batch = []
                if batch:
                    yield "".join(batch)
        finally:
            os.unlink(path)


def stream_responses(
    spill: Spill, client_id: UUID, header: str, batch_lines: int = 200_000
) -> Iterator[Response]:
    """Stream a UC's spilled result as Response chunks: header on the first,
    `last=True` only on the final one, keeping memory bounded to ~2 chunks."""
    chunks = spill.iter_chunks_and_clear(client_id, batch_lines)
    prev = next(chunks, None)
    if prev is None:
        yield Response(client_id, header + "\n")
        return
    prev = header + prev
    for nxt in chunks:
        yield Response(client_id, prev, last=False)
        prev = nxt
    yield Response(client_id, prev + "\n")
