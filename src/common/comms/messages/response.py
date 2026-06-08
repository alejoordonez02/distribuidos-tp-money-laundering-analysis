from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class Response(Message):
    def __init__(self, client_id: UUID, body: str, last: bool = True, uc_id: int = 0):
        self.client_id = client_id
        self.uc_id = uc_id
        self.body = body
        self.last = last

    @classmethod
    def _type(cls):
        return MessageType.RESPONSE

    def _fields(self) -> list[Any]:
        return [self.client_id, self.uc_id, self.body, self.last]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id, uc_id, body, last = fields
        client_id = UUID(client_id)
        return cls(client_id, body, last, uc_id)
