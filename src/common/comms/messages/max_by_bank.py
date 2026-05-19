from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class MaxByBank(Message):

    def __init__(self, client_id: UUID, data: dict[str, tuple[str, float]]):
        self.client_id = client_id
        self.data = data  # bank_id → (account, max_amount)

    @classmethod
    def _type(cls):
        return MessageType.MAX_BY_BANK

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[[bank_id, account, amount] for bank_id, (account, amount) in self.data.items()],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        data = {e[0]: (e[1], float(e[2])) for e in fields[1:]}
        return cls(client_id, data)
