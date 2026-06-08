from typing import Any, Self
from uuid import UUID

from common.data import Transaction, fast_datetime

from .message import Message
from .message_types import MessageType


class Transactions(Message):
    def __init__(self, client_id: UUID, transactions: list[Transaction]):
        self.client_id = client_id
        self.transactions = transactions

    @classmethod
    def _type(cls):
        return MessageType.TRANSACTIONS

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[
                [
                    t.timestamp,
                    t.from_bank,
                    t.from_account,
                    t.to_bank,
                    t.to_account,
                    t.amount_received,
                    t.receiving_currency,
                    t.amount_paid,
                    t.payment_currency,
                    t.payment_format,
                ]
                for t in self.transactions
            ],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        transactions = [
            Transaction(fast_datetime(t_fields[0]), *t_fields[1:])
            for t_fields in fields[1:]
        ]

        return cls(client_id, transactions)
