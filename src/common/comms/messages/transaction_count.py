from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class TransactionCount(Message):
    def __init__(self, client_id: UUID, count: int):
        self.client_id = client_id
        self.count = count

    @classmethod
    def _type(cls):
        return MessageType.COUNT

    def _fields(self) -> list[Any]:
        return [self.client_id, self.count]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        return cls(UUID(fields[0]), int(fields[1]))
