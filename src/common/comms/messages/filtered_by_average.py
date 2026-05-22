from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class FilteredByAverage(Message):
    def __init__(self, client_id: UUID, entries: list[tuple[str, str, str, float]]):
        self.client_id = client_id
        self.entries = entries  # Lista de (bank_id, account, payment_foramt, amount)

    @classmethod
    def _type(cls):
        return MessageType.FILTERED_BY_AVERAGE

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[[bank_id, account, payment_foramt, amount] for bank_id, account, payment_foramt, amount in self.entries],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        entries = [(e[0], e[1], e[2], float(e[3])) for e in fields[1:]]
        return cls(client_id, entries)
