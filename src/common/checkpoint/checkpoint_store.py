import os
import tempfile
from typing import Any, Optional

import msgpack

from common.fault_injection import maybe_crash


class CheckpointStore:
    """Atomic on-disk checkpoint: writes to a temp file + os.replace, so a crash
    mid-write never leaves a partial checkpoint. No fsync: the fault model is a
    process crash (RabbitMQ stable), and the atomic rename + OS page cache survive
    that. fsync (for power loss) is out of scope."""

    def __init__(self, path: str):
        self._path = path

    def save(self, blob: dict[str, Any]):
        directory = os.path.dirname(self._path) or "."
        os.makedirs(directory, exist_ok=True)

        fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(msgpack.packb(blob, use_bin_type=True))
            # crash here only leaks a temp file; os.replace below swaps the real checkpoint atomically
            maybe_crash("during_checkpoint_write")
            os.replace(tmp, self._path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def load(self) -> Optional[dict[str, Any]]:
        if not os.path.exists(self._path):
            return None
        with open(self._path, "rb") as f:
            return msgpack.unpackb(f.read(), raw=False, strict_map_key=False)
