from typing import Any, Self

from .message import Message
from .message_types import MessageType


class Hello(Message):
    """Empty handshake the client sends first to open a connection.

    The gateway replies with a ``HelloAck`` carrying the id it minted for the
    client.
    """

    @classmethod
    def _type(cls):
        return MessageType.HELLO

    def _fields(self) -> list[Any]:
        return []

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        return cls()
