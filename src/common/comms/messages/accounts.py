from typing import Any, Self
from uuid import UUID

from common.data import Account

from .message import Message
from .message_types import MessageType


class Accounts(Message):
    def __init__(self, client_id: UUID, accounts: list[Account]):
        self.client_id = client_id
        self.accounts = accounts

    @classmethod
    def _type(cls):
        return MessageType.ACCOUNTS

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[
                [a.bank_name, a.bank_id, a.account_number, a.entity_id, a.entity_name]
                for a in self.accounts
            ],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        accounts = [Account(*a_fields) for a_fields in fields[1:]]
        return cls(client_id, accounts)
