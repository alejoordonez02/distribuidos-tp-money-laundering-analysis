from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class RingDone(Message):
    def __init__(self, client_id: UUID, origin: int, count: int):
        self.client_id = client_id
        self.origin = origin
        self.processed_count = count

    @classmethod
    def _type(cls):
        return MessageType.RING_DONE

    def _fields(self) -> list[Any]:
        return [self.client_id, self.origin, self.processed_count]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        return cls(client_id, *fields[1:])
