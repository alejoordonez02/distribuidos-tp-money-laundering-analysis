from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class EOF(Message):
    def __init__(
        self,
        client_id: UUID,
        processed_count: int = -1,
        expected_count: int = -1,
        next_expected_count: int = -1,
        origin: int = -1,
    ):
        self.client_id = client_id
        self.processed_count = processed_count
        self.expected_count = expected_count
        self.next_expected_count = next_expected_count
        self.origin = origin

    @classmethod
    def _type(cls):
        return MessageType.EOF

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            self.processed_count,
            self.expected_count,
            self.next_expected_count,
            self.origin,
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        return cls(client_id, *fields[1:])
