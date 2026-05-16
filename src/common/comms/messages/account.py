from typing import Any, Self

from .message import Message
from .message_types import MessageType


class Account(Message):
    def __init__(
        self,
        bank_name,
        bank_id,
        account_number,
        entity_id,
        entity_name,
    ):
        self.bank_name = bank_name
        self.bank_id = bank_id
        self.account_number = account_number
        self.entity_id = entity_id
        self.entity_name = entity_name

    @classmethod
    def _type(cls):
        return MessageType.ACCOUNT

    def _fields(self) -> list[Any]:
        return [
            self.bank_name,
            self.bank_id,
            self.account_number,
            self.entity_id,
            self.entity_name,
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        bank_name, bank_id, account_number, entity_id, entity_name = fields
        return cls(bank_name, bank_id, account_number, entity_id, entity_name)
