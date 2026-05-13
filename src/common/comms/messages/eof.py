from typing import Any, Self

# from uuid import UUID
from .message import Message
from .message_types import MessageType


# este eof me lo robé de mi tp de coordinación.
# Lo subo como ej de cómo usar la interfaz `Message`, la idea
# es q se haga todo arriba y acá sólo pasar los fields. Justo
# este tiene un pasito extra que es instanciar el `UUID` en
# `_from_fields`
class EOF(Message):
    def type(self) -> MessageType:
        return MessageType.EOF

    @classmethod
    def deserialize(cls, bytes2: bytes) -> Self:
        return cls._deserialize(bytes2)  # type: ignore

    @classmethod
    def _type(cls):
        return MessageType.EOF

    def _fields(self) -> list[Any]:
        return [MessageType.EOF.value]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        return cls()
