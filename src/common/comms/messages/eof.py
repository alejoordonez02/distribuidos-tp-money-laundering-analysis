from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class EOF(Message):
    def __init__(self, client_id: UUID):
        self.client_id = client_id

    @classmethod
    def _type(cls):
        return MessageType.EOF

    def _fields(self) -> list[Any]:
        return [MessageType.EOF, self.client_id]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = fields[0]
        client_id = UUID(client_id)
        return cls(client_id)
