import json
import logging
from collections import defaultdict
from typing import Any, Iterable
from uuid import UUID

from common.checkpoint import MultiShardSpill
from common.comms.messages import Graph, HighDegree, Node

from .aggregate_fn import AggregateFn

MIN_DEGREE = 5
MAX_AMOUNT = 100_000
SHARDING_FILES = 500


def _sharding_hash(node: str) -> int:
    return hash(node) % SHARDING_FILES


def _serialize(node: str, out: set[str], in_: set[str]) -> str:
    return json.dumps([node, list(out), list(in_)])


def _deserialize(line: str) -> tuple[str, set[str], set[str]]:
    raw = json.loads(line)
    return raw[0], set(raw[1]), set(raw[2])


def _capped_update(store: dict[str, set[str]], node: str, neighbors: set[str]):
    bucket = store.setdefault(node, set())
    for neighbor in neighbors:
        if len(bucket) >= MIN_DEGREE:
            break
        bucket.add(neighbor)


class UC4Degree(AggregateFn):
    def __init__(self, spill: MultiShardSpill):
        self._out: dict[UUID, dict[str, set[str]]] = {}
        self._in: dict[UUID, dict[str, set[str]]] = {}
        self._spill = spill

    def aggregate(self, msg: Graph):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._out:
            self._out[msg.client_id] = {}
            self._in[msg.client_id] = {}

        for node, (predecessors, successors) in msg.nodes.items():
            node = str(node)
            _capped_update(self._in[msg.client_id], node, {str(n) for n in predecessors})
            _capped_update(self._out[msg.client_id], node, {str(n) for n in successors})

        if len(self._out[msg.client_id]) >= MAX_AMOUNT:
            self._downstream(msg.client_id)

    def get_result(self, client_id: UUID) -> Iterable[tuple[HighDegree, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        if client_id in self._out:
            self._downstream(client_id)

        out: dict[str, set[str]] = defaultdict(set)
        in_: dict[str, set[str]] = defaultdict(set)
        for shard in self._spill.shards_of(client_id):
            for line in self._spill.read_shard(client_id, shard):
                node, o, i = _deserialize(line)
                _capped_update(out, node, o)
                _capped_update(in_, node, i)

        hi_out = {Node.from_str(n) for n, s in out.items() if len(s) >= MIN_DEGREE}
        hi_in = {Node.from_str(n) for n, s in in_.items() if len(s) >= MIN_DEGREE}

        yield HighDegree(client_id, hi_out, hi_in), 0

    def discard(self, client_id: UUID):
        self._spill.clear(client_id)
        self._out.pop(client_id, None)
        self._in.pop(client_id, None)

    def snapshot_state(self) -> dict[str, Any]:
        for client_id in list(self._out.keys()):
            self._downstream(client_id)
        return self._spill.snapshot_state()

    def restore_state(self, snapshot: dict[str, Any]):
        self._spill.restore_state(snapshot)
        self._out = {}
        self._in = {}

    def clear_stale_spill(self):
        self._spill.clear_all()

    def _downstream(self, client_id: UUID):
        out = self._out.pop(client_id, {})
        in_ = self._in.pop(client_id, {})
        logging.info("spilling degree to disk")

        for node in set(out) | set(in_):
            self._spill.append(
                client_id,
                _sharding_hash(node),
                _serialize(node, out.get(node, set()), in_.get(node, set())) + "\n",
            )
