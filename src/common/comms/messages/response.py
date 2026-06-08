from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class Response(Message):
    def __init__(self, client_id: UUID, body: str, last: bool = True):
        self.client_id = client_id
        self.body = body
        # A UC's result is streamed as one or more Response chunks; `last` marks
        # the final chunk so the client knows that UC is complete. Big UCs (UC1,
        # UC3) send many chunks so no single message exceeds RabbitMQ's
        # max_message_size; small UCs send one chunk (last=True).
        self.last = last

    @classmethod
    def _type(cls):
        return MessageType.RESPONSE

    def _fields(self) -> list[Any]:
        return [self.client_id, self.body, self.last]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id, body, last = fields
        client_id = UUID(client_id)
        return cls(client_id, body, last)
