from typing import Any, Self
from uuid import UUID

from .graph_src import Node
from .message import Message
from .message_types import MessageType

def _node_from_str(s: str) -> Node:
    bank, account = s.split(",")
    return Node(bank, account)

class NodeMsg(Message):
    def __init__(self, client_id: UUID, node: Node, predecesors: set[Node], succesors: set[Node]):
        self.client_id = client_id
        self.node = node
        self.predecesors = predecesors
        self.succesors = succesors


    @classmethod
    def _type(cls):
        return MessageType.NODEMSG

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            str(self.node),
            [str(n) for n in self.predecesors],
            [str(n) for n in self.succesors],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        return cls(
            client_id=UUID(fields[0]),
            node=_node_from_str(fields[1]),
            predecesors={_node_from_str(n) for n in fields[2]},
            succesors={_node_from_str(n) for n in fields[3]},
        )
