from uuid import UUID

from common.comms.messages import Graph, Node, Path, PathCounts

from .group_by_fn import GroupByFn


class UC4CountPaths(GroupByFn):
    def __init__(self):
        self._preds: dict[UUID, dict[Node, set[Node]]] = {}
        self._succs: dict[UUID, dict[Node, set[Node]]] = {}

    def group_by(self, msg: Graph):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._preds:
            self._preds[msg.client_id] = {}
            self._succs[msg.client_id] = {}

        for node, (p, s) in msg.nodes.items():
            self._preds[msg.client_id].setdefault(node, set()).update(p)
            self._succs[msg.client_id].setdefault(node, set()).update(s)

    def get_result(self, client_id: UUID) -> PathCounts:
        preds = self._preds.pop(client_id, {})
        succs = self._succs.pop(client_id, {})

        # Only count paths through nodes that have BOTH predecessors and
        # successors after merging all partial graphs. This is the filter
        # that was previously (incorrectly) applied inside each compute_graph
        # node before the merge, which caused nodes whose two edges landed on
        # different workers to be silently dropped.
        result = PathCounts(client_id, {})
        for node, node_preds in preds.items():
            node_succs = succs.get(node)
            if not node_succs:
                continue
            for a in node_preds:
                for c in node_succs:
                    if a != c:
                        result.add(Path(a, c), 1)
        return result
