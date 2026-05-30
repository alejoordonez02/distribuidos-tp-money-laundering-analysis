from common.comms.messages import Graph, Node, Transactions

from .group_by_fn import GroupByFn


class UC4ComputeGraph(GroupByFn):
    def group_by(self, msg: Transactions) -> Graph:  # type: ignore[reportIncompatibleMethodOverride]
        graph = Graph(msg.client_id, {})

        for t in msg.transactions:
            a = Node(t.from_bank, t.from_account)
            b = Node(t.to_bank, t.to_account)

            graph.add_origin(b, a)
            graph.add_destination(a, b)

        return graph
