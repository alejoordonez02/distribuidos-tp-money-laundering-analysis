from typing import Any, Self

from .message import Message
from .message_types import MessageType


class SupervisorLeader(Message):
    def __init__(self, idx: int):
        self.idx = idx

    @classmethod
    def _type(cls):
        return MessageType.SUPERVISOR_LEADER

    def _fields(self) -> list[Any]:
        return [self.idx]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        idx = fields[0]
        return cls(idx)
