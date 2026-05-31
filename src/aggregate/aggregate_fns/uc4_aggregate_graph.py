import json
import logging
import os
import pathlib
import tempfile
from collections import defaultdict
from typing import Iterable
from uuid import UUID

from common.comms.messages import Graph, Node, NodeMsg

from .aggregate_fn import AggregateFn

MAX_AMOUNT = 100000
SHARDING_FILES = 1000

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
    def __init__(self):
        self._preds: dict[UUID, dict[Node, set[Node]]] = {}
        self._succs: dict[UUID, dict[Node, set[Node]]] = {}
        self._files: dict[UUID, dict[int, pathlib.Path]] = {}

    def _file_for(self, node: Node, client_id: UUID) -> pathlib.Path:
        hash = sharding_hash(node)
        if client_id not in self._files:
            self._files[client_id] = {}
        if hash not in self._files[client_id]:
            fd, path = tempfile.mkstemp(
                prefix=f"UC4:{client_id}:{hash}", suffix=".jsonl"
            )
            os.close(fd)
            self._files[client_id][hash] = pathlib.Path(path)
        return self._files[client_id][hash]

    def aggregate(self, msg: Graph):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._preds:
            self._preds[msg.client_id] = {}
            self._succs[msg.client_id] = {}

        for node, (predecessors, successors) in msg.nodes.items():
            self._preds[msg.client_id].setdefault(node, set()).update(predecessors)
            self._succs[msg.client_id].setdefault(node, set()).update(successors)

        # Sharding
        if len(self._preds[msg.client_id]) >= MAX_AMOUNT:
            self.downstream(msg.client_id)

    def get_result(self, client_id: UUID) -> Iterable[tuple[Graph, int]]:
        if client_id not in self._succs and client_id not in self._files:
            return ()

        if client_id in self._succs:
            self.downstream(client_id)

        for _, file in self._files[client_id].items():
            preds: dict[Node, set[Node]] = defaultdict(set)
            succs: dict[Node, set[Node]] = defaultdict(set)

            with open(file, "r") as f:
                for line in f:
                    node, pre, suc = _deserialize(line)
                    preds[node].update(pre)
                    succs[node].update(suc)
            file.unlink()

            affinities: dict[int, Graph] = defaultdict(lambda: Graph(client_id, {}))

            for node in preds.keys():
                affinity_shard_idx = hash(node) % AFFINITY_SHARDS
                affinity_shard = affinities[affinity_shard_idx]

                if node not in affinity_shard.nodes:
                    affinity_shard.nodes[node] = (preds[node], succs[node])
                    continue

                affinities[affinity_shard_idx].nodes[node][0].update(preds[node])
                affinities[affinity_shard_idx].nodes[node][1].update(succs[node])

            for affinity, graph in affinities.items():
                yield graph, affinity

        self._files.pop(client_id)
        self._preds.pop(client_id)
        self._succs.pop(client_id)

    def downstream(self, client_id):
        logging.info("Downstreaming")
        preds = self._preds[client_id]
        succs = self._succs[client_id]
        for shard in range(SHARDING_FILES):
            for node, _ in preds.items():
                if sharding_hash(node) != shard:
                    continue
                path = self._file_for(node, client_id)
                with open(path, "a") as f:
                    f.write(_serialize(node, preds[node], succs[node]) + "\n")
        self._preds[client_id].clear()
        self._succs[client_id].clear()
