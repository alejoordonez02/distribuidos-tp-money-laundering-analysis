import json
import logging
import math
import os
import pathlib
import tempfile
from collections import defaultdict
from typing import Iterable
from uuid import UUID

from common.comms.messages import Graph, Node

from .aggregate_fn import AggregateFn

MAX_AMOUNT = 100_000
SHARDING_FILES = 500

AFFINITY_SHARDS = 100

# Salting threshold (in pairs). A node whose len(preds) * len(succs) exceeds this
# is a hub: counting its cross product on a single count_paths instance is a
# mono-core / OOM bottleneck. Such nodes are split into a grid of (preds_chunk,
# succs_chunk) tiles routed to different instances. Each tile holds ~this many
# pairs; pick it below count_paths' MAX_AMOUNT so a tile never forces a spill.
SPLIT_THRESHOLD = 250_000


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
                node_preds = preds[node]
                node_succs = succs[node]

                # Salting: split a hub's cross product into a grid of tiles so it
                # is counted in parallel across instances instead of mono-core on
                # one. Each tile is a disjoint slice of (a, c) pairs (disjoint in
                # both a and c), so aggregate_paths' additive sum reconstructs the
                # exact same counts. Tiles are emitted as their own messages via
                # yield, so the controller's add_sent_data_count tracks them and
                # the downstream EOF expected_count stays correct automatically.
                if len(node_preds) * len(node_succs) > SPLIT_THRESHOLD:
                    yield from self._salted_tiles(client_id, node, node_preds, node_succs)
                    continue

                affinity_shard_idx = hash(node) % AFFINITY_SHARDS
                affinities[affinity_shard_idx].nodes[node] = (node_preds, node_succs)

            for affinity, graph in affinities.items():
                yield graph, affinity

        self._files.pop(client_id)
        # FIXME: está ok este default? hace falta
        #        este pop?
        self._preds.pop(client_id, None)
        self._succs.pop(client_id, None)

    def _salted_tiles(
        self, client_id: UUID, node: Node, node_preds: set[Node], node_succs: set[Node]
    ) -> Iterable[tuple[Graph, int]]:
        """Yield (Graph, affinity) tiles that grid-split a hub's cross product.

        Tiles keep the node's preds:succs aspect ratio and hold ~SPLIT_THRESHOLD
        pairs each. A pred lands in exactly one preds-chunk and a succ in exactly
        one succs-chunk, so every (a, c) pair is produced by exactly one tile — no
        double counting. Tiles spread across instances by hashing (node, index) so
        the hub no longer piles onto a single shard.
        """
        p_count, s_count = len(node_preds), len(node_succs)
        chunk_p = max(1, int(math.sqrt(SPLIT_THRESHOLD * p_count / s_count)))
        chunk_s = max(1, int(math.sqrt(SPLIT_THRESHOLD * s_count / p_count)))
        preds_list = list(node_preds)
        succs_list = list(node_succs)

        tile = 0
        for pi in range(0, p_count, chunk_p):
            pchunk = set(preds_list[pi : pi + chunk_p])
            for si in range(0, s_count, chunk_s):
                schunk = set(succs_list[si : si + chunk_s])
                affinity = hash((str(node), tile)) % AFFINITY_SHARDS
                yield Graph(client_id, {node: (pchunk, schunk)}), affinity
                tile += 1

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
