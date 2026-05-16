from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class Response(Message):
    def __init__(self, client_id: UUID, body: str):
        self.client_id = client_id
        self.body = body

    @classmethod
    def deserialize(cls, bytes2: bytes) -> Self:
        return cls._deserialize(bytes2)  # type: ignore

    @classmethod
    def _type(cls):
        return MessageType.RESPONSE

    def _fields(self) -> list[Any]:
        return [MessageType.RESPONSE.value, self.client_id, self.body]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id, body = fields
        client_id = UUID(client_id)
        return cls(client_id, body)
