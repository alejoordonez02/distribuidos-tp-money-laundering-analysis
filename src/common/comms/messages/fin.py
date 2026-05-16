from typing import Any, Self

from .message import Message
from .message_types import MessageType


class FIN(Message):
    @classmethod
    def _type(cls):
        return MessageType.FIN

    def _fields(self) -> list[Any]:
        return [MessageType.FIN]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        return cls()
