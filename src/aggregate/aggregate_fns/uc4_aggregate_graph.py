import json
import logging
import os
import pathlib
import tempfile
from collections import defaultdict
from typing import Iterable
from uuid import UUID

from common.comms.messages import Graph, Node

from .aggregate_fn import AggregateFn

MAX_AMOUNT = 100_000
SHARDING_FILES = 200

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

    def _file_for_shard(self, shard: int, client_id: UUID) -> pathlib.Path:
        if client_id not in self._files:
            self._files[client_id] = {}
        if shard not in self._files[client_id]:
            fd, path = tempfile.mkstemp(
                prefix=f"UC4:{client_id}:{shard}", suffix=".jsonl"
            )
            os.close(fd)
            self._files[client_id][shard] = pathlib.Path(path)
        return self._files[client_id][shard]

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
        # FIXME: está ok este default? hace falta
        #        este pop?
        self._preds.pop(client_id, None)
        self._succs.pop(client_id, None)

    def downstream(self, client_id):
        logging.info("writing in memory graphs in disk")
        preds = self._preds[client_id]
        succs = self._succs[client_id]

        # Bucket nodes by shard in a single O(N) pass, then write each shard file
        # once. The previous O(SHARDING_FILES * N) scan with one open() per node
        # blocked the pika event loop long enough for RabbitMQ to drop the
        # connection (writer send_failed,timeout).
        by_shard: dict[int, list[Node]] = defaultdict(list)
        for node in preds.keys():
            by_shard[sharding_hash(node)].append(node)

        for shard, nodes in by_shard.items():
            file = self._file_for_shard(shard, client_id)
            with open(file, "a") as f:
                f.writelines(
                    _serialize(node, preds[node], succs[node]) + "\n"
                    for node in nodes
                )

        self._preds.pop(client_id, None)
        self._succs.pop(client_id, None)
