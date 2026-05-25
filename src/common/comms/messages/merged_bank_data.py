from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class MergedBankData(Message):
    def __init__(self, client_id: UUID, entries: list[tuple[str, str, float, str]]):
        self.client_id = client_id
        self.entries = entries  # Lista de (bank_id, account, max_amount, bank_name)

    @classmethod
    def _type(cls):
        return MessageType.MERGED_BANK_DATA

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[
                [bank_id, account, amount, bank_name]
                for bank_id, account, amount, bank_name in self.entries
            ],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        entries = [
            (bank_id, account, amount, bank_name)
            for bank_id, account, amount, bank_name in fields[1:]
        ]

        return cls(client_id, entries)
