from typing import Any, Self

from .message import Message
from .message_types import MessageType


class Hello(Message):
    """
    Empty handshake the client sends first to request a connection.

    The client does NOT pick its own id anymore — the gateway mints one and
    returns it in a ``HelloAck``, then the client stamps that gateway-assigned id
    on every message. The gateway still forwards data messages raw (it trusts the
    id it minted), so the passthrough is preserved while ids stay collision-free
    and controlled by the gateway.
    """

    @classmethod
    def _type(cls):
        return MessageType.HELLO

    def _fields(self) -> list[Any]:
        return []

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        return cls()
