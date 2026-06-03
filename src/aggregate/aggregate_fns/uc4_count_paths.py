import json
import logging
import os
import tempfile
from collections import defaultdict
from pathlib import Path as FilePath
from typing import Iterable
from uuid import UUID

from common.comms.messages import Graph, Path, PathMsg
from common.comms.messages.message import MessageJSONEncoder
from common.comms.messages.path_count import PathCounts

from .aggregate_fn import AggregateFn

MAX_AMOUNT = 100_000
SHARDING_FILES = 200
AFFINITY_SHARDS = 10


def _sharding_hash(path: Path, client_id: UUID) -> int:
    return hash(f"{client_id}_{str(path)}") % SHARDING_FILES


def _serialize(p: PathMsg) -> str:
    return json.dumps(p._fields(), cls=MessageJSONEncoder)


def _deserialize(line: str) -> tuple[Path, int]:
    p = PathMsg._from_fields(json.loads(line))
    return p.path, p.counts


class UC4CountPaths(AggregateFn):
    def __init__(self):
        self._paths: dict[UUID, dict[Path, int]] = {}
        self._files: dict[UUID, dict[int, FilePath]] = {}

    def _file_for(self, path: Path, client_id: UUID) -> FilePath:
        shard = _sharding_hash(path, client_id)
        if client_id not in self._files:
            self._files[client_id] = {}
        if shard not in self._files[client_id]:
            fd, file_path = tempfile.mkstemp(
                prefix=f"UC4CountPaths:{client_id}:{shard}", suffix=".jsonl"
            )
            os.close(fd)
            self._files[client_id][shard] = FilePath(file_path)
        return self._files[client_id][shard]

    def aggregate(self, msg: Graph):  # type: ignore[reportIncompatibleMethodOverride]
        client_id = msg.client_id
        if client_id not in self._paths:
            self._paths[client_id] = {}

        paths = self._paths[client_id]
        for _, (preds, succs) in msg.nodes.items():
            for a in preds:
                for c in succs:
                    if a != c:
                        path = Path(a, c)
                        paths[path] = paths.get(path, 0) + 1

        if len(paths) >= MAX_AMOUNT:
            self._downstream(client_id)

    def get_result(self, client_id: UUID) -> Iterable[tuple[PathCounts, int]]:
        if client_id not in self._paths and client_id not in self._files:
            return ()

        if client_id in self._paths:
            self._downstream(client_id)

        for _, file in self._files[client_id].items():
            paths: dict[Path, int] = {}

            with open(file, "r") as f:
                for line in f:
                    path, count = _deserialize(line)
                    paths[path] = paths.get(path, 0) + count

            file.unlink()

            affinities: dict[int, PathCounts] = defaultdict(
                lambda: PathCounts(client_id, {})
            )
            for path, count in paths.items():
                affinities[hash(path) % AFFINITY_SHARDS].add(path, count)

            for affinity, path_counts in affinities.items():
                yield path_counts, affinity

        self._files.pop(client_id)
        self._paths.pop(client_id, None)

    def _downstream(self, client_id: UUID):
        logging.info("spilling count_paths to disk")
        paths = self._paths[client_id]
        for shard in range(SHARDING_FILES):
            for p, count in paths.items():
                if _sharding_hash(p, client_id) != shard:
                    continue
                file = self._file_for(p, client_id)
                with open(file, "a") as f:
                    f.write(_serialize(PathMsg(client_id, p, count)) + "\n")
        self._paths.pop(client_id)
