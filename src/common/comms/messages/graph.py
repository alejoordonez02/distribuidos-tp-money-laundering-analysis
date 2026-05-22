from typing import Any, Self
from uuid import UUID

from .graph_src import Node
from .message import Message
from .message_types import MessageType


class Graph(Message):
    def __init__(self, client_id: UUID, nodes: dict[Node, tuple[set[Node], set[Node]]]):
        self.client_id = client_id
        self.nodes = nodes

    def add_origin(self, node: Node, origin: Node):
        if node not in self.nodes:
            self.nodes[node] = (set(), set())

        self.nodes[node][0].add(origin)

    def add_destination(self, node: Node, destination: Node):
        if node not in self.nodes:
            self.nodes[node] = (set(), set())

        self.nodes[node][1].add(destination)

    @classmethod
    def _type(cls):
        return MessageType.GRAPH

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            [
                (
                    node.fields(),
                    [p.fields() for p in predecessors],
                    [s.fields() for s in successors],
                )
                for node, (predecessors, successors) in self.nodes.items()
            ],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        nodes = {
            Node(nbank, naccount): (
                set([Node(pbank, paccount) for (pbank, paccount) in predecessors]),
                set([Node(sbank, saccount) for (sbank, saccount) in successors]),
            )
            for ((nbank, naccount), predecessors, successors) in fields[1]
        }

        return cls(client_id, nodes)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Graph):
            return False

        for node, (predecessors, successors) in other.nodes.items():
            if node not in self.nodes:
                return False
            if predecessors != self.nodes[node][0]:
                return False
            if successors != self.nodes[node][1]:
                return False

        return True
