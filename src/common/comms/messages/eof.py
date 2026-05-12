from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


# este eof me lo robé de mi tp de coordinación.
# Lo subo como ej de cómo usar la interfaz `Message`, la idea
# es q se haga todo arriba y acá sólo pasar los fields. Justo
# este tiene un pasito extra que es instanciar el `UUID` en
# `_from_fields`
class EOF(Message):
    def __init__(self, client_id: UUID):
        self.client_id = client_id

    def type(self) -> MessageType:
        return MessageType.EOF

    @classmethod
    def deserialize(cls, bytes2: bytes) -> Self:
        return cls._deserialize(bytes2)  # type: ignore

    @classmethod
    def _type(cls):
        return MessageType.EOF

    def _fields(self) -> list[Any]:
        return [MessageType.EOF.value, self.client_id]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = fields
        client_id = UUID(client_id)  # type: ignore
        return cls(client_id)
