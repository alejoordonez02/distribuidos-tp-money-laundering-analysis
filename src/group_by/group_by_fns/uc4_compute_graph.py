from uuid import UUID

from common.comms.messages import Edges, Node, Transactions
from common.pipeline_config import UC4_EDGES_BATCH_SIZE

from .group_by_fn import GroupByFn


class UC4ComputeGraph(GroupByFn):
    def __init__(self):
        self.client_edges: dict[UUID, set[tuple[Node, Node]]] = {}

    def group_by(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self.client_edges:
            self.client_edges[msg.client_id] = set()

        for t in msg.transactions:
            origin = Node(t.from_bank, t.from_account)
            destination = Node(t.to_bank, t.to_account)
            self.client_edges[msg.client_id].add((origin, destination))

    def get_result(self, client_id: UUID) -> list[Edges]:
        edges = list(self.client_edges.pop(client_id, set()))
        return [
            Edges(client_id, edges[i : i + UC4_EDGES_BATCH_SIZE])
            for i in range(0, max(len(edges), 1), UC4_EDGES_BATCH_SIZE)
        ]
