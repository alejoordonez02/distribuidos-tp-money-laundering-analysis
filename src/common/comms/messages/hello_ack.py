from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class HelloAck(Message):
    """
    Sent by the gateway in reply to a client's ``Hello``: carries the
    gateway-minted ``client_id``. The client stamps this id on every subsequent
    message, so the gateway can keep forwarding data raw (no per-batch parse)
    while owning the id namespace (no collisions / no client-chosen ids).
    """

    def __init__(self, client_id: UUID):
        self.client_id = client_id

    @classmethod
    def _type(cls):
        return MessageType.HELLO_ACK

    def _fields(self) -> list[Any]:
        return [self.client_id]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        return cls(UUID(fields[0]))
