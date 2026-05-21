from uuid import UUID

from common.comms.messages import Graph, Transactions

from .group_by_fn import GroupByFn


class UC4ComputeGraph(GroupByFn):
    def __init__(self):
        self.graphs: dict[UUID, Graph] = {}

    def aggregate(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self.graphs:
            self.graphs[msg.client_id] = Graph(msg.client_id, {})

        client_nodes = self.graphs[msg.client_id].nodes
        for t in msg.transactions:
            origin = t.from_bank + t.from_account
            destination = t.to_bank + t.to_account

            if origin not in client_nodes:
                client_nodes[origin] = (set(), set())
            if destination not in client_nodes:
                client_nodes[destination] = (set(), set())

            client_nodes[origin][1].add(destination)
            client_nodes[destination][1].add(origin)

    def get_result(self, client_id: UUID) -> Graph:
        if client_id not in self.graphs:
            return Graph(client_id, {})

        return self.graphs.pop(client_id)
