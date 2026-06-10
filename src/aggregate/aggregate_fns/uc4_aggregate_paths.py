import json
import logging
from collections import defaultdict
from typing import Any, Iterable
from uuid import UUID

from common.checkpoint import MultiShardSpill
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
    def __init__(self, spill: MultiShardSpill):
        self._paths: dict[UUID, dict[Path, int]] = {}
        self._spill = spill

    def aggregate(self, msg: PathCounts):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._paths:
            self._paths[msg.client_id] = {}

        for path, count in msg.counts.items():
            paths = self._paths[msg.client_id]
            paths[path] = paths.get(path, 0) + count

        if len(self._paths[msg.client_id]) >= MAX_AMOUNT:
            self.downstream(msg.client_id)

    def get_result(self, client_id: UUID) -> Iterable[tuple[PathCounts, int]]:
        if client_id in self._paths:
            self.downstream(client_id)

        for shard in self._spill.shards_of(client_id):
            paths: dict[Path, int] = defaultdict(int)
            for line in self._spill.read_shard(client_id, shard):
                path, amount = _deserialize(line)
                paths[path] += amount

            qualifying = {
                path: count
                for path, count in paths.items()
                if count >= MIN_PATH_COUNT
            }

            yield PathCounts(client_id, qualifying), 0

        self._spill.clear(client_id)
        self._paths.pop(client_id, None)

    def snapshot_state(self) -> dict[str, Any]:
        for client_id in list(self._paths.keys()):
            self.downstream(client_id)
        return self._spill.snapshot_state()

    def restore_state(self, snapshot: dict[str, Any]):
        self._spill.restore_state(snapshot)
        self._paths = {}

    def downstream(self, client_id: UUID):
        logging.info("writing in memory paths in disk")
        paths = self._paths[client_id]

        by_shard: dict[int, list[tuple[Path, int]]] = defaultdict(list)
        for p, amount in paths.items():
            by_shard[sharding_hash(p, client_id)].append((p, amount))

        for shard, items in by_shard.items():
            for p, amount in items:
                self._spill.append(
                    client_id, shard, _serialize(PathMsg(client_id, p, amount)) + "\n"
                )

        self._paths.pop(client_id)
