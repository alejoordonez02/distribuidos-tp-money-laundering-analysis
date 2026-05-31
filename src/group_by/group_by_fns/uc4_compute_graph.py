from typing import Iterable

from common.comms.messages import Graph, Node, NodeMsg, Transactions

from .group_by_fn import GroupByFn


class UC4ComputeGraph(GroupByFn):
    def group_by(self, msg: Transactions) -> Iterable[tuple[NodeMsg, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        graph = Graph(msg.client_id, {})
        for t in msg.transactions:
            a = Node(t.from_bank, t.from_account)
            b = Node(t.to_bank, t.to_account)

            graph.add_origin(b, a)
            graph.add_destination(a, b)

        # TODO: acá vamos a tener q cambiar el msj
        #       y rutear por nodos b. Le dejo 0
        #       ahora porque igual no vamos a
        #       escalar el próximo controller hasta
        #       tener bien el msj.
        # return ((graph, 0),)
        for node, (p, s) in graph.nodes.items():
            yield NodeMsg(msg.client_id, node, p, s), hash(node)
