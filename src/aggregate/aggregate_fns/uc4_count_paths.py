import json
import logging
from collections import defaultdict
from typing import Any, Iterable
from uuid import UUID

from common.checkpoint import MultiShardSpill
from common.comms.messages import Graph, Path, PathMsg
from common.comms.messages.message import MessageJSONEncoder
from common.comms.messages.path_count import PathCounts

from .aggregate_fn import AggregateFn

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


class UC4CountPaths(AggregateFn):
    def __init__(self, spill: MultiShardSpill):
        self._paths: dict[UUID, dict[Path, int]] = {}
        self._spill = spill

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
        if client_id in self._paths:
            self._downstream(client_id)

        for shard in self._spill.shards_of(client_id):
            paths: dict[Path, int] = {}
            for line in self._spill.read_shard(client_id, shard):
                path, count = _deserialize(line)
                paths[path] = paths.get(path, 0) + count

            affinities: dict[int, PathCounts] = defaultdict(
                lambda: PathCounts(client_id, {})
            )
            for path, count in paths.items():
                affinities[hash(path) % AFFINITY_SHARDS].add(path, count)

            for affinity, path_counts in affinities.items():
                yield path_counts, affinity

        self._spill.clear(client_id)
        self._paths.pop(client_id, None)

    def snapshot_state(self) -> dict[str, Any]:
        # Force the in-memory partials to disk, then the checkpoint only records
        # each shard's committed length (keeps the checkpoint small).
        for client_id in list(self._paths.keys()):
            self._downstream(client_id)
        return self._spill.snapshot_state()

    def restore_state(self, snapshot: dict[str, Any]):
        self._spill.restore_state(snapshot)
        self._paths = {}

    def _downstream(self, client_id: UUID):
        logging.info("spilling count_paths to disk")
        paths = self._paths[client_id]

        by_shard: dict[int, list[tuple[Path, int]]] = defaultdict(list)
        for p, count in paths.items():
            by_shard[_sharding_hash(p, client_id)].append((p, count))

        for shard, items in by_shard.items():
            for p, count in items:
                self._spill.append(
                    client_id, shard, _serialize(PathMsg(client_id, p, count)) + "\n"
                )

        self._paths.pop(client_id)
