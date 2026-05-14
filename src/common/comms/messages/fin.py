from typing import Any, Self

from .message import Message
from .message_types import MessageType


class FIN(Message):
    def type(self) -> MessageType:
        return MessageType.FIN

    @classmethod
    def deserialize(cls, bytes2: bytes) -> Self:
        return cls._deserialize(bytes2)  # type: ignore

    @classmethod
    def _type(cls):
        return MessageType.FIN

    def _fields(self) -> list[Any]:
        return [MessageType.FIN.value]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        return cls()
