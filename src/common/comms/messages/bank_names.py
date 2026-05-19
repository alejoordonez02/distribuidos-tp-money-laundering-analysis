from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class BankNames(Message):
   
    def __init__(self, client_id: UUID, data: dict[str, str]):
        self.client_id = client_id
        self.data = data  # bank_id → bank_name

    @classmethod
    def _type(cls):
        return MessageType.BANK_NAMES

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[[bank_id, bank_name] for bank_id, bank_name in self.data.items()],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        data = {e[0]: e[1] for e in fields[1:]}
        return cls(client_id, data)
