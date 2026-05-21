from datetime import datetime
from typing import Any, Self
from uuid import UUID

from common.data import Transaction

from .message import Message
from .message_types import MessageType

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


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
            Transaction(datetime.strptime(t_fields[0], DATETIME_FORMAT), *t_fields[1:])
            for t_fields in fields[2:]
        ]

        return cls(client_id, transactions)
