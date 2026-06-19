from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class Abort(Message):
    """Signals that a client crashed before completing its input, so every node must
    drop that client's partial state instead of waiting for an EOF that never comes."""

    def __init__(self, client_id: UUID):
        self.client_id = client_id

    @classmethod
    def _type(cls):
        return MessageType.ABORT

    def _fields(self) -> list[Any]:
        return [self.client_id]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        return cls(client_id)
