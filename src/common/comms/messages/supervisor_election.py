from typing import Any, Self

from .message import Message
from .message_types import MessageType


class SupervisorElection(Message):
    @classmethod
    def _type(cls):
        return MessageType.SUPERVISOR_ELECTION

    def _fields(self) -> list[Any]:
        return []

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        return cls()
