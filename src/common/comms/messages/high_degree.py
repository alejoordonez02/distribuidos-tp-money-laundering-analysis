from typing import Any, Self
from uuid import UUID

from .graph_src import Node
from .message import Message
from .message_types import MessageType


class HighDegree(Message):
    def __init__(self, client_id: UUID, hi_out: set[Node], hi_in: set[Node]):
        self.client_id = client_id
        self.hi_out = hi_out  # accounts with out_degree >= MIN
        self.hi_in = hi_in  # accounts with in_degree >= MIN

    @classmethod
    def _type(cls):
        return MessageType.HIGH_DEGREE

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            [n.fields() for n in self.hi_out],
            [n.fields() for n in self.hi_in],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        hi_out = {Node(bank, account) for (bank, account) in fields[1]}
        hi_in = {Node(bank, account) for (bank, account) in fields[2]}
        return cls(client_id, hi_out, hi_in)
