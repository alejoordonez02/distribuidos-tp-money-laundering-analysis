from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class Graph(Message):
    def __init__(self, client_id: UUID, nodes: dict[str, tuple[set[str], set[str]]]):
        """
        Returns a `Graph` message.

        # Args
        * client_id: the ID of the client to which the message corresponds.
        * nodes: the nodes in the graph, {node, (predecessors, successors)}

        # Returns
        A new `Graph` message instance.
        """
        self.client_id = client_id
        self.nodes = nodes

    @classmethod
    def _type(cls):
        return MessageType.GRAPH

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            [(n, [n for n in p], [n for n in s]) for (n, (p, s)) in self.nodes.items()],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = fields[0]
        nodes = {n: (set(p), set(s)) for (n, (p, s)) in fields}
        return cls(client_id, nodes)
