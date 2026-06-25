import json
import logging
from collections import defaultdict
from typing import Any, Iterable
from uuid import UUID

from common.checkpoint import MultiShardSpill
from common.comms.messages import Graph, Node

from .aggregate_fn import AggregateFn

SPILL_THRESHOLD = 100_000
SHARDING_FILES = 500

AFFINITY_SHARDS = 100


def sharding_hash(node: Node) -> int:
    return hash(f"{str(node)}") % SHARDING_FILES


def _serialize(node: Node, preds: set[Node], succs: set[Node]) -> str:
    return json.dumps([str(node), [str(n) for n in preds], [str(n) for n in succs]])


def _deserialize(line: str) -> tuple[Node, set[Node], set[Node]]:
    raw = json.loads(line)
    return (
        Node.from_str(raw[0]),
        {Node.from_str(n) for n in raw[1]},
        {Node.from_str(n) for n in raw[2]},
    )


class UC4AggregateGraphs(AggregateFn):
    def __init__(self, spill: MultiShardSpill):
        self._preds: dict[UUID, dict[Node, set[Node]]] = {}
        self._succs: dict[UUID, dict[Node, set[Node]]] = {}
        self._spill = spill

    def aggregate(self, msg: Graph):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._preds:
            self._preds[msg.client_id] = {}
            self._succs[msg.client_id] = {}

        for node, (predecessors, successors) in msg.nodes.items():
            self._preds[msg.client_id].setdefault(node, set()).update(predecessors)
            self._succs[msg.client_id].setdefault(node, set()).update(successors)

        if len(self._preds[msg.client_id]) >= SPILL_THRESHOLD:
            self._spill_to_disk(msg.client_id)

    def get_result(self, client_id: UUID) -> Iterable[tuple[Graph, int]]:
        if client_id in self._succs:
            self._spill_to_disk(client_id)

        for shard in self._spill.shards_of(client_id):
            preds: dict[Node, set[Node]] = defaultdict(set)
            succs: dict[Node, set[Node]] = defaultdict(set)
            for line in self._spill.read_shard(client_id, shard):
                node, pre, suc = _deserialize(line)
                preds[node].update(pre)
                succs[node].update(suc)

            affinities: dict[int, Graph] = defaultdict(lambda: Graph(client_id, {}))
            for node in preds.keys():
                affinity_shard_idx = hash(node) % AFFINITY_SHARDS
                affinities[affinity_shard_idx].nodes[node] = (preds[node], succs[node])

            for affinity, graph in affinities.items():
                yield graph, affinity

        self._preds.pop(client_id, None)
        self._succs.pop(client_id, None)

    def discard(self, client_id: UUID):
        self._spill.clear(client_id)
        self._preds.pop(client_id, None)
        self._succs.pop(client_id, None)

    def snapshot_state(self) -> dict[str, Any]:
        for client_id in list(self._preds.keys()):
            self._spill_to_disk(client_id)
        return self._spill.snapshot_state()

    def restore_state(self, snapshot: dict[str, Any]):
        self._spill.restore_state(snapshot)
        self._preds = {}
        self._succs = {}

    def clear_stale_spill(self):
        self._spill.clear_all()

    def _spill_to_disk(self, client_id):
        logging.info("writing in memory graphs in disk")
        preds = self._preds[client_id]
        succs = self._succs[client_id]

        by_shard: dict[int, list[Node]] = defaultdict(list)
        for node in preds.keys():
            by_shard[sharding_hash(node)].append(node)

        for shard, nodes in by_shard.items():
            for node in nodes:
                self._spill.append(
                    client_id, shard, _serialize(node, preds[node], succs[node]) + "\n"
                )

        self._preds.pop(client_id, None)
        self._succs.pop(client_id, None)
