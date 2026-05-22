from uuid import UUID

from common.comms.messages import Graph, Node, Transactions

from .group_by_fn import GroupByFn


class UC4ComputeGraph(GroupByFn):
    def __init__(self):
        self.graphs: dict[UUID, Graph] = {}

    def group_by(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self.graphs:
            self.graphs[msg.client_id] = Graph(msg.client_id, {})

        for t in msg.transactions:
            origin = Node(t.from_bank, t.from_account)
            destination = Node(t.to_bank, t.to_account)

            self.graphs[msg.client_id].add_origin(destination, origin)
            self.graphs[msg.client_id].add_destination(origin, destination)

    def get_result(self, client_id: UUID) -> Graph:
        if client_id not in self.graphs:
            return Graph(client_id, {})

        return self.graphs.pop(client_id)
