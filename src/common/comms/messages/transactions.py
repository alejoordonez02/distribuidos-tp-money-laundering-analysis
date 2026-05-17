from typing import Any, Self

from common.data import Transaction

from .message import Message
from .message_types import MessageType


class Transactions(Message):
    def __init__(self, transactions: list[Transaction]):
        self.transactions = transactions

    @classmethod
    def _type(cls):
        return MessageType.TRANSACTIONS

    def _fields(self) -> list[Any]:
        return [
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
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        transactions = [Transaction(*t_fields) for t_fields in fields]
        return cls(transactions)
