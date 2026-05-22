from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class MergedTransactions(Message):
    def __init__(self, client_id: UUID, entries: list[tuple[str, str, str, float, float]]):
        self.client_id = client_id
        self.entries = entries  # Lista de (bank_id, account, payment_format, amount, average)

    @classmethod
    def _type(cls):
        return MessageType.MERGED_TRANSACTIONS

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[[bank_id, account, payment_format, amount, average] for bank_id, account, payment_format, amount, average in self.entries],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        entries = [(e[0], e[1], e[2], float(e[3]), float(e[4])) for e in fields[1:]]
        return cls(client_id, entries)
