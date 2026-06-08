import os
import tempfile
from typing import Iterator
from uuid import UUID

from common.comms.messages import Response


class LineSpill:
    """Append-only on-disk buffer of pre-formatted response lines, keyed by client.

    The join nodes that materialize one result line per transaction (UC1, UC3)
    accumulate every line until the client's EOF arrives. On the Large dataset
    that is millions of lines (UC1 ~7.7M, UC3 ~3.5M); keeping the underlying
    `Transaction` objects in RAM OOMs the node, so each line is written straight
    to a temp file as it is produced and read back only at EOF.

    Mirrors the tempfile-based spill idiom used by `UC4CountPaths` in the
    aggregate node, but stripped down: the join only appends and replays an
    ordered log, so there is no per-key merge, threshold or sharding — RAM stays
    O(1) by construction (one buffered write handle per client).
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
        """Yield the spilled lines in batches of `batch_lines` (each a single
        string), then delete the file. Memory stays bounded to one batch."""
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
    spill: LineSpill, client_id: UUID, header: str, batch_lines: int = 200_000
) -> Iterator[Response]:
    """Stream a UC's result from its spill as Response chunks: header on the
    first, trailing newline on the last, ``last=True`` only on the final chunk.
    Uses a 1-chunk lookahead so no single message exceeds the broker's
    max_message_size and memory stays bounded to ~2 chunks."""
    chunks = spill.iter_chunks_and_clear(client_id, batch_lines)
    prev = next(chunks, None)
    if prev is None:
        yield Response(client_id, header + "\n")  # empty result → one (last) chunk
        return
    prev = header + prev
    for nxt in chunks:
        yield Response(client_id, prev, last=False)
        prev = nxt
    yield Response(client_id, prev + "\n")  # final chunk (last=True by default)
