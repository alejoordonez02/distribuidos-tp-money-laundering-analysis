from typing import Any, Self

from .message import Message
from .message_types import MessageType


class SupervisorElection(Message):
    def __init__(
        self,
        node_idxs: list[int],
    ):
        self.node_idxs = node_idxs

    def add_node_idx(self, idx: int):
        self.node_idxs.append(idx)

    @classmethod
    def _type(cls):
        return MessageType.ELECTION

    def _fields(self) -> list[Any]:
        return [self.node_idxs]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        node_idxs = fields[0]
        return cls(node_idxs)
