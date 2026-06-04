import json
import logging
import os
import tempfile
from collections import defaultdict
from pathlib import Path as FilePath
from typing import Iterable
from uuid import UUID

from common.comms.messages import Path, PathMsg
from common.comms.messages.message import MessageJSONEncoder
from common.comms.messages.path_count import PathCounts

from .aggregate_fn import AggregateFn

MAX_AMOUNT = 100_000
SHARDING_FILES = 500
MIN_PATH_COUNT = 5


def sharding_hash(path: Path, client_id: UUID) -> int:
    return hash(f"{client_id:}_{str(path)}") % SHARDING_FILES


def _serialize(p: PathMsg) -> str:
    return json.dumps(p._fields(), cls=MessageJSONEncoder)


def _deserialize(line: str) -> tuple[Path, int]:
    p = PathMsg._from_fields(json.loads(line))
    return p.path, p.counts


class UC4AggregatePaths(AggregateFn):
    def __init__(self):
        self._paths: dict[UUID, dict[Path, int]] = {}
        self._files: dict[UUID, dict[int, FilePath]] = {}

    def _file_for_shard(self, shard: int, client_id: UUID) -> FilePath:
        if client_id not in self._files:
            self._files[client_id] = {}

        if shard not in self._files[client_id]:
            fd, file_path = tempfile.mkstemp(prefix=f"UC4:{shard}", suffix=".jsonl")
            os.close(fd)
            self._files[client_id][shard] = FilePath(file_path)

        return self._files[client_id][shard]

    def aggregate(self, msg: PathCounts):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._paths:
            self._paths[msg.client_id] = {}

        for path, count in msg.counts.items():
            paths = self._paths[msg.client_id]
            paths[path] = paths.get(path, 0) + count

        if len(self._paths[msg.client_id]) >= MAX_AMOUNT:
            self.downstream(msg.client_id)

    def get_result(self, client_id: UUID) -> Iterable[tuple[PathCounts, int]]:
        if client_id not in self._paths and client_id not in self._files:
            return ()

        if client_id in self._paths:
            self.downstream(client_id)

        for _, file in self._files[client_id].items():
            paths: dict[Path, int] = defaultdict()

            with open(file, "r") as f:
                for line in f:
                    path, amount = _deserialize(line)

                    if path not in paths:
                        paths[path] = 0

                    paths[path] += amount

            file.unlink()

            qualifying = {
                path: count
                for path, count in paths.items()
                if count >= MIN_PATH_COUNT
            }

            yield PathCounts(client_id, qualifying), 0

        self._files.pop(client_id)

    def downstream(self, client_id: UUID):
        logging.info("writing in memory paths in disk")
        paths = self._paths[client_id]

        # Bucket paths by shard in a single O(N) pass, then write each shard file
        # once. The previous O(SHARDING_FILES * N) scan with one open() per path
        # blocked the pika event loop long enough for RabbitMQ to drop the
        # connection (writer send_failed,timeout).
        by_shard: dict[int, list[tuple[Path, int]]] = defaultdict(list)
        for p, amount in paths.items():
            by_shard[sharding_hash(p, client_id)].append((p, amount))

        for shard, items in by_shard.items():
            file = self._file_for_shard(shard, client_id)
            with open(file, "a") as f:
                f.writelines(
                    _serialize(PathMsg(client_id, p, amount)) + "\n"
                    for p, amount in items
                )

        self._paths.pop(client_id)
