import logging

from common.comms.messages import Node, Transactions, NodeMsg, Graph

from .group_by_fn import GroupByFn


class UC4ComputeGraph(GroupByFn):
    def group_by(self, msg: Transactions) -> Iterator[NodeMsg]:  # type: ignore[reportIncompatibleMethodOverride]
        graph = Graph(msg.client_id, {})
        for t in msg.transactions:
            a = Node(t.from_bank, t.from_account)
            b = Node(t.to_bank, t.to_account)

            graph.add_origin(b, a)
            graph.add_destination(a, b)

        for node, (p, s) in graph.nodes.items():
            yield NodeMsg(msg.client_id, node, p, s)
