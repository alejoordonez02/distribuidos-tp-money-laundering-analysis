from collections import defaultdict
from uuid import UUID

from common.comms.messages import Edges, Node, Path, PathCounts

from .group_by_fn import GroupByFn

_BATCH_SIZE = 50_000


class UC4CountPaths(GroupByFn):
    def __init__(self):
        self.client_succs: dict[UUID, dict[int, set[int]]] = {}
        self.client_preds: dict[UUID, dict[int, set[int]]] = {}
        self.client_node_ids: dict[UUID, dict[str, int]] = {}
        self.client_nodes: dict[UUID, list[Node]] = {}

    def _node_id(self, client_id: UUID, node: Node) -> int:
        node_ids = self.client_node_ids[client_id]
        if node.key not in node_ids:
            nodes = self.client_nodes[client_id]
            node_ids[node.key] = len(nodes)
            nodes.append(node)
        return node_ids[node.key]

    def group_by(self, msg: Edges):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self.client_succs:
            self.client_succs[msg.client_id] = defaultdict(set)
            self.client_preds[msg.client_id] = defaultdict(set)
            self.client_node_ids[msg.client_id] = {}
            self.client_nodes[msg.client_id] = []

        succs = self.client_succs[msg.client_id]
        preds = self.client_preds[msg.client_id]

        for origin_node, middle_node in msg.edges:
            o_id = self._node_id(msg.client_id, origin_node)
            m_id = self._node_id(msg.client_id, middle_node)
            succs[o_id].add(m_id)
            preds[m_id].add(o_id)

    def get_result(self, client_id: UUID) -> list[PathCounts]:
        if client_id not in self.client_succs:
            return [PathCounts(client_id, {})]

        succs = self.client_succs.pop(client_id)
        preds = self.client_preds.pop(client_id)
        nodes = self.client_nodes.pop(client_id)
        self.client_node_ids.pop(client_id)

        # For each middle node B: for each origin A in preds[B] and each end C in succs[B],
        # count B as one intermediary for the pair (A, C).
        # Using int counts avoids the memory overhead of storing intermediary sets.
        counts: dict[tuple[int, int], int] = defaultdict(int)
        for b, origins in preds.items():
            ends = succs.get(b)
            if not ends:
                continue
            for a in origins:
                for c in ends:
                    if a != c:
                        counts[(a, c)] += 1

        if not counts:
            return [PathCounts(client_id, {})]

        batches: list[PathCounts] = []
        batch = PathCounts(client_id, {})
        batch_size = 0

        for (origin_id, end_id), count in counts.items():
            batch.add(Path(nodes[origin_id], nodes[end_id]), count)
            batch_size += 1
            if batch_size >= _BATCH_SIZE:
                batches.append(batch)
                batch = PathCounts(client_id, {})
                batch_size = 0

        if batch_size > 0:
            batches.append(batch)

        return batches
