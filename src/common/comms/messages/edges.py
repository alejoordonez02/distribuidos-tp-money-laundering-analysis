from typing import Any, Self
from uuid import UUID

from .graph_src import Node
from .message import Message
from .message_types import MessageType


class Edges(Message):
    def __init__(self, client_id: UUID, edges: list[tuple[Node, Node]]):
        self.client_id = client_id
        self.edges = edges

    @classmethod
    def _type(cls):
        return MessageType.EDGES

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            [[a.fields(), b.fields()] for a, b in self.edges],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        edges = [
            (Node(a[0], a[1]), Node(b[0], b[1]))
            for a, b in fields[1]
        ]
        return cls(client_id, edges)
