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

from .stateful_fn import StatefulFn

# Pairs held in memory before spilling. Raised now that aggregate_graphs salts
# hubs into ~SPLIT_THRESHOLD-sized tiles: no single message can dump a mega-hub
# here anymore, so we can keep more in RAM (fewer, larger spills — and small/
# perfect datasets stay entirely in memory) while staying well within 8GB.
MAX_AMOUNT = 500_000
SHARDING_FILES = 500
AFFINITY_SHARDS = 10


def _sharding_hash(path: Path, client_id: UUID) -> int:
    return hash(f"{client_id}_{str(path)}") % SHARDING_FILES


def _serialize(p: PathMsg) -> str:
    return json.dumps(p._fields(), cls=MessageJSONEncoder)


def _deserialize(line: str) -> tuple[Path, int]:
    p = PathMsg._from_fields(json.loads(line))
    return p.path, p.counts


class UC4CountPaths(StatefulFn):
    def __init__(self):
        self._paths: dict[UUID, dict[Path, int]] = {}
        self._files: dict[UUID, dict[int, FilePath]] = {}

    def _file_for_shard(self, shard: int, client_id: UUID) -> FilePath:
        if client_id not in self._files:
            self._files[client_id] = {}
        if shard not in self._files[client_id]:
            fd, file_path = tempfile.mkstemp(
                prefix=f"UC4CountPaths:{client_id}:{shard}", suffix=".jsonl"
            )
            os.close(fd)
            self._files[client_id][shard] = FilePath(file_path)
        return self._files[client_id][shard]

    def transform(self, msg: Graph):  # type: ignore[reportIncompatibleMethodOverride]
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

                # Spill mid-message: a single hub node generates len(preds) *
                # len(succs) pairs in one Graph message, which can be millions —
                # far past MAX_AMOUNT — before the per-message check below would
                # ever run, blowing up RAM. Checking per predecessor caps the
                # in-memory dict regardless of hub size. _downstream pops
                # self._paths[client_id], so re-create and re-bind paths after it.
                # Counts stay correct: spilled partials are summed back per path
                # in get_result, so the result is identical to never spilling.
                if len(paths) >= MAX_AMOUNT:
                    self._downstream(client_id)
                    self._paths[client_id] = {}
                    paths = self._paths[client_id]

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

        # Bucket paths by shard in a single O(N) pass, then write each shard file
        # once. Avoids the O(SHARDING_FILES * N) scan + one open() per path that
        # used to block the pika event loop long enough for RabbitMQ to drop us.
        by_shard: dict[int, list[tuple[Path, int]]] = defaultdict(list)
        for p, count in paths.items():
            by_shard[_sharding_hash(p, client_id)].append((p, count))

        for shard, items in by_shard.items():
            file = self._file_for_shard(shard, client_id)
            with open(file, "a") as f:
                f.writelines(
                    _serialize(PathMsg(client_id, p, count)) + "\n"
                    for p, count in items
                )

        self._paths.pop(client_id)
