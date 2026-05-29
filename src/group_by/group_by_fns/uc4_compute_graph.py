from collections import defaultdict
from uuid import UUID

from common.comms.messages import Graph, Node, Transactions

from .group_by_fn import GroupByFn


class UC4ComputeGraph(GroupByFn):
    def __init__(self):
        self._succs: dict[UUID, dict[Node, set[Node]]] = {}
        self._preds: dict[UUID, dict[Node, set[Node]]] = {}

    def group_by(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._succs:
            self._succs[msg.client_id] = defaultdict(set)
            self._preds[msg.client_id] = defaultdict(set)

        succs = self._succs[msg.client_id]
        preds = self._preds[msg.client_id]

        for t in msg.transactions:
            origin = Node(t.from_bank, t.from_account)
            dest = Node(t.to_bank, t.to_account)
            succs[origin].add(dest)
            preds[dest].add(origin)

    def get_result(self, client_id: UUID) -> Graph:
        succs = self._succs.pop(client_id, {})
        preds = self._preds.pop(client_id, {})

        nodes = {
            node: (preds.get(node, set()), succs.get(node, set()))
            for node in succs.keys() | preds.keys()
        }

        return Graph(client_id, nodes)
