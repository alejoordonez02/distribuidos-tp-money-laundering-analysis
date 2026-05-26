from collections import defaultdict
from dataclasses import dataclass, field
from uuid import UUID

from common.comms.messages import Graph, Node, Path, PathCounts
from common.pipeline_config import UC4_PATHS_BATCH_SIZE

from .group_by_fn import GroupByFn


@dataclass
class _ClientState:
    node_ids: dict[str, int] = field(default_factory=dict)
    nodes: list[Node] = field(default_factory=list)
    counts: dict[tuple[int, int], int] = field(default_factory=lambda: defaultdict(int))


class UC4CountPaths(GroupByFn):
    def __init__(self):
        self._clients: dict[UUID, _ClientState] = {}

    def _node_id(self, state: _ClientState, node: Node) -> int:
        if node.key not in state.node_ids:
            state.node_ids[node.key] = len(state.nodes)
            state.nodes.append(node)
        return state.node_ids[node.key]

    def group_by(self, msg: Graph):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._clients:
            self._clients[msg.client_id] = _ClientState()

        state = self._clients[msg.client_id]

        for node_b, (preds, succs) in msg.nodes.items():
            if not preds or not succs:
                continue
            b_id = self._node_id(state, node_b)
            pred_ids = [self._node_id(state, p) for p in preds]
            succ_ids = [self._node_id(state, s) for s in succs]
            for a in pred_ids:
                for c in succ_ids:
                    if a != c:
                        state.counts[(a, c)] += 1

    def get_result(self, client_id: UUID) -> list[PathCounts]:  # type: ignore[reportIncompatibleMethodOverride]
        state = self._clients.pop(client_id, None)
        if state is None or not state.counts:
            return [PathCounts(client_id, {})]

        batches: list[PathCounts] = []
        batch = PathCounts(client_id, {})
        batch_size = 0

        for (origin_id, end_id), count in state.counts.items():
            batch.add(Path(state.nodes[origin_id], state.nodes[end_id]), count)
            batch_size += 1
            if batch_size >= UC4_PATHS_BATCH_SIZE:
                batches.append(batch)
                batch = PathCounts(client_id, {})
                batch_size = 0

        if batch_size > 0:
            batches.append(batch)

        return batches
